local fiber = require('fiber')
local log = require('log')
local fio = require('fio')

local function check_error(func, ...)
    local pstatus, status, err = pcall(func, ...)
    if pstatus then
        return status, err
    end
    err = string.gsub(status, '.*/[a-z]+.lua.*[0-9]+: ', '')
    return err
end

local function shuffle_masters(cfg)
    for replicaset_uuid, replicaset in pairs(cfg.sharding) do
        local old_master = nil
        local new_master = nil
        for instance_uuid, replica in pairs(replicaset.replicas) do
            if replica.master then
                old_master = replica
            else
                new_master = replica
            end
        end
        old_master.master = nil
        new_master.master = true
    end
end

function collect_timeouts(rs)
    local timeouts = {}
    for uuid, replica in pairs(rs.replicas) do
        table.insert(timeouts, {ok = replica.net_sequential_ok,
                    fail = replica.net_sequential_fail,
                    timeout = replica.net_timeout})
    end
    return timeouts
end

local function wait_master(test_run, replicaset, master)
    log.info('Waiting until slaves are connected to a master')
    local all_is_ok
    while true do
        all_is_ok = true
        for _, replica in pairs(replicaset) do
            if replica == master then
                goto continue
            end
            local info = test_run:eval(replica, 'box.info.replication')
            if #info == 0 or #info[1] < 2 then
                all_is_ok = false
                goto continue
            end
            info = info[1]
            for _, replica_info in pairs(info) do
                local upstream = replica_info.upstream
                if upstream and upstream.status ~= 'follow' then
                    all_is_ok = false
                    goto continue
                end
            end
::continue::
        end
        if not all_is_ok then
            fiber.sleep(0.1)
        else
            break
        end
    end
    log.info('Slaves are connected to a master "%s"', master)
end

--
-- Check that data has at least all etalon's fields and they are
-- equal.
-- @param etalon Table which fields should be found in `data`.
-- @param data Table which is checked against `etalon`.
--
-- @retval Boolean indicator of equality and if is not equal, then
--         table of names of fields which are different in `data`.
--
local function has_same_fields(etalon, data)
    assert(type(etalon) == 'table' and type(data) == 'table')
    local diff = {}
    for k, v in pairs(etalon) do
        if v ~= data[k] then
            table.insert(diff, k)
        end
    end
    if #diff > 0 then
        return false, diff
    end
    return true
end

-- Git directory of the project. Used in evolution tests to
-- fetch old versions of vshard.
local SOURCEDIR = os.getenv('PACKPACK_GIT_SOURCEDIR')
if not SOURCEDIR then
    SOURCEDIR = os.getenv('SOURCEDIR')
end
if not SOURCEDIR then
    local script_path = debug.getinfo(1).source:match("@?(.*/)")
    script_path = fio.abspath(script_path)
    SOURCEDIR = fio.abspath(script_path .. '/../../../')
end

local BUILDDIR = os.getenv('BUILDDIR')
if not BUILDDIR then
    BUILDDIR = SOURCEDIR
end

return {
    check_error = check_error,
    shuffle_masters = shuffle_masters,
    collect_timeouts = collect_timeouts,
    wait_master = wait_master,
    has_same_fields = has_same_fields,
    SOURCEDIR = SOURCEDIR,
    BUILDDIR = BUILDDIR,
}
