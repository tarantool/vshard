local fiber = require('fiber')
local log = require('log')
local fio = require('fio')

local name_to_uuid = {
    storage_1_a = '8a274925-a26d-47fc-9e1b-af88ce939412',
    storage_1_b = '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
    storage_2_a = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    storage_2_b = '001688c3-66f8-4a31-8e19-036c17d489c2',
    storage_3_a = 'ee34807e-be5c-4ae3-8348-e97be227a305',

    storage_1_1_a = '32a2d4b8-f146-44ed-9d51-2436507efdf8',
    storage_1_1_b = 'c1c849b1-641d-40b8-9283-bcfe73d46270',
    storage_1_2_a = '04e677ed-c7ba-47e0-a67f-b5100cfa86af',
    storage_1_2_b = 'c7a979ee-9263-4a38-84a5-2fb6a0a32684',
    storage_2_1_a = '88dc03f0-23fb-4f05-b462-e29186542864',
    storage_2_1_b = '4230b711-f5c4-4131-bf98-88cd43a16901',
    storage_2_2_a = '6b1eefbc-1e2e-410e-84ff-44c572ea9916',
    storage_2_2_b = 'be74419a-1e56-4ba4-97e9-6b18710f63c5',
}

local box_uuid = {
    box_1_a = '3e01062d-5c1b-4382-b14e-f80a517cb462',
    box_1_b = 'db778aec-267f-47bb-9347-49828232c8db',
    box_1_c = '84705440-4433-11e9-b210-d663bd873d93',
    box_2_a = '7223fc89-1a0d-480b-a33e-a8d2b117b13d',
    box_2_b = '56bb8450-9526-442b-ba96-b96cc38ee2f9',
    box_3_a = 'ad40a200-730e-401a-9400-30dbd96dedbd',
    box_3_b = '434ec511-4a3d-4a68-b613-fc5475ef5f6b',
    box_4_a = '535df17b-c325-466c-9320-77f1190c749c',
    box_4_b = 'f24d5101-adec-48b5-baa9-ace33abfd10f'
}

for k, v in pairs(box_uuid) do
    name_to_uuid[k] = v
    name_to_uuid['full'..k] = v
end

local replicasets = {'cbf06940-0790-498b-948d-042b62cf3d29',
                     'ac522f65-aa94-4134-9f64-51ee384f1a54',
                     '910ee49b-2540-41b6-9b8c-c976bef1bb17',
                     'dd208fb8-8b90-49bc-8393-6b3a99da7c52'}

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

--
-- Apply @a command on each node of @a cluster consisting from
-- a list of replicasets.
-- @param test_run Test run instance.
-- @param cluster List of replicasets. Each replicaset is a list
--        of instance names.
-- @param command Command to execute.
-- @param ... Arguments for string.format to format @a command.
--
local function map_evals(test_run, cluster, command, ...)
    for _, rs in pairs(cluster) do
        for _, node in pairs(rs) do
            test_run:eval(node, string.format(command, ...))
        end
    end
end

--
-- Filter out from test output replication lag and idle, UUIDs.
--
local function push_rs_filters(test_run)
    test_run:cmd("push filter 'lag: .+' to 'lag: <lag>'")
    test_run:cmd("push filter 'idle: .+' to 'idle: <idle>'")
    for name, uuid in pairs(name_to_uuid) do
        test_run:cmd("push filter '"..uuid.."' to '<"..name..">'")
    end
    for i, uuid in pairs(replicasets) do
        test_run:cmd("push filter '"..uuid.."' to '<replicaset_"..i..">'")
    end
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
    map_evals = map_evals,
    push_rs_filters = push_rs_filters,
    name_to_uuid = name_to_uuid,
    replicasets = replicasets,
    SOURCEDIR = SOURCEDIR,
    BUILDDIR = BUILDDIR,
}
