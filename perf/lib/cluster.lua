local clock = require('clock')
local fiber = require('fiber')
local fio = require('fio')
local net_box = require('net.box')
local popen = require('popen')
local config = require('perf.lib.config')
local util = require('perf.lib.util')
local verror = require('vshard.error')

-- Allow generators to prepare before the shared measurement window.
local measurement_start_delay = 0.25

local function cluster_call(cluster, node, name, args, timeout)
    return cluster.connections[node.name]:call(name, args, {
        timeout = timeout or cluster.cfg.startup_timeout_seconds,
    })
end

local function cluster_check_alive(cluster)
    for name, handle in pairs(cluster.processes) do
        assert(handle.status.state == 'alive',
               'Instance exited: ' .. name ..
               ' (see ' .. cluster.directory .. ')')
    end
end

local function cluster_wait(cluster, description, condition, timeout)
    local deadline = clock.monotonic() + timeout
    repeat
        cluster_check_alive(cluster)
        local ok, value = pcall(condition)
        if ok and value then
            return
        end
        if not ok then
            local err = verror.make(value)
            if type(err) ~= 'table' or
               (not verror.is_timeout(err) and
                err.code ~= box.error.NO_CONNECTION) then
                error(value)
            end
        end
        fiber.sleep(0.01)
    until clock.monotonic() >= deadline
    error('Timed out waiting for ' .. description)
end

local function cluster_spawn(cluster, node, index)
    local workdir = fio.pathjoin(cluster.directory, node.name)
    assert(fio.mktree(workdir))
    local argv = {
        cluster.binary,
        fio.pathjoin(cluster.root, 'perf/instance.lua'),
        fio.pathjoin(cluster.directory, 'cluster.json'),
        tostring(index),
        cluster.root,
    }
    local handle = assert(popen.new(argv, {
        stdin = popen.opts.DEVNULL,
        stdout = popen.opts.DEVNULL,
        setsid = true,
        group_signal = true,
    }))
    cluster.processes[node.name] = handle
end

local function cluster_connect(cluster, node)
    local conn = net_box.connect(node.uri, {
        wait_connected = false, reconnect_after = 0.05,
    })
    cluster.connections[node.name] = conn
    cluster_wait(cluster, node.name .. ' readiness', function()
        if not conn:is_connected() then
            return false
        end
        local ok, value = pcall(cluster_call, cluster, node, 'perf.ready',
                                {}, 0.1)
        if not ok then
            return false
        end
        return value.name == node.name
    end, cluster.cfg.startup_timeout_seconds)
end

local function cluster_sync(cluster)
    assert(cluster_call(cluster, cluster.router, 'vshard.router.sync', {
        cluster.cfg.startup_timeout_seconds,
    }))
end

local function cluster_start(cluster)
    assert(fio.mktree(cluster.directory))
    util.write_json(fio.pathjoin(cluster.directory, 'cluster.json'), {
        config = cluster.cfg,
        nodes = cluster.nodes,
        sharding = config.sharding(cluster.cfg, cluster.nodes),
    })
    for index, node in ipairs(cluster.nodes) do
        if node.role ~= 'generator' then
            cluster_spawn(cluster, node, index)
            cluster_connect(cluster, node)
            if node.role == 'router' then
                cluster.router = cluster.router or node
            end
        end
    end
    assert(cluster_call(cluster, cluster.router, 'vshard.router.bootstrap', {{
        timeout = cluster.cfg.startup_timeout_seconds,
    }}))
    local rows, buckets = 0, 0
    for _, node in ipairs(cluster.nodes) do
        if node.master then
            local info = assert(cluster_call(cluster, node, 'perf.preload'))
            rows, buckets = rows + info.rows, buckets + info.buckets
        end
    end
    cluster_sync(cluster)
    assert(rows == cluster.cfg.rows, 'Unexpected total row count')
    assert(buckets == cluster.cfg.bucket_count, 'Unexpected total buckets')
    for _, node in ipairs(cluster.nodes) do
        if node.role == 'router' then
            cluster_wait(cluster, node.name .. ' discovery', function()
                local info = cluster_call(cluster, node, 'vshard.router.info',
                                          {}, 0.5)
                return info.bucket.available_rw == cluster.cfg.bucket_count
            end, cluster.cfg.startup_timeout_seconds)
        end
    end
    for index, node in ipairs(cluster.nodes) do
        if node.role == 'generator' then
            cluster_spawn(cluster, node, index)
            cluster_connect(cluster, node)
        end
    end
end

local function cluster_measure(cluster, case, duration, diagnostics)
    local start_at = clock.monotonic() + measurement_start_delay
    if diagnostics then
        local processes = {}
        for name, handle in pairs(cluster.processes) do
            processes[name] = handle.pid
        end
        -- Match pidstat's wall-clock timestamps to the monotonic window.
        local start_time = clock.time() + start_at - clock.monotonic()
        util.write_json(fio.pathjoin(cluster.directory, 'diagnostics.json'), {
            start_time = start_time,
            end_time = start_time + duration,
            processes = processes,
        })
    end
    -- One timeout to drain the last request, one to return generator results.
    local deadline = start_at + duration + 2 * cluster.cfg.rpc_timeout_seconds
    local futures, results = {}, {}
    for _, node in ipairs(cluster.nodes) do
        if node.role == 'generator' then
            futures[node.name] = cluster.connections[node.name]:call(
                'perf.run', {case, start_at, duration}, {is_async = true})
        end
    end
    assert(clock.monotonic() < start_at,
           'Generators missed the measurement barrier')
    for name, future in pairs(futures) do
        local response, err = future:wait_result(
            math.max(0, deadline - clock.monotonic()))
        assert(response, name .. ': ' .. tostring(err))
        results[name] = assert(response[1], name .. ': no result')
        cluster_check_alive(cluster)
    end
    return results
end

local function cluster_stop(cluster)
    local connections, processes = cluster.connections, cluster.processes
    cluster.connections, cluster.processes = {}, {}
    for _, conn in pairs(connections) do
        pcall(conn.close, conn)
    end
    for _, handle in pairs(processes) do
        if handle.status.state == 'alive' then
            pcall(handle.terminate, handle)
        end
    end
    -- Tarantool 2.11's popen:wait() ignores timeout options.
    -- Give all children one grace period; close() kills those still alive.
    local deadline = clock.monotonic() + 3
    for _, handle in pairs(processes) do
        while handle.status.state == 'alive' and
              clock.monotonic() < deadline do
            fiber.sleep(0.01)
        end
        pcall(handle.close, handle)
    end
end

local function cluster_cleanup(cluster)
    for _, node in ipairs(cluster.nodes) do
        assert(fio.rmtree(fio.pathjoin(cluster.directory, node.name)))
    end
end

local cluster_mt = {
    __index = {
        start = cluster_start,
        sync = cluster_sync,
        measure = cluster_measure,
        stop = cluster_stop,
        cleanup = cluster_cleanup,
    },
}

local function cluster_new(cfg, root, binary, directory)
    return setmetatable({
        cfg = cfg,
        root = root,
        binary = binary,
        directory = directory,
        nodes = config.topology(cfg),
        processes = {},
        connections = {},
    }, cluster_mt)
end

return {
    new = cluster_new,
}
