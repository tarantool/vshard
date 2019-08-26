local ffi = require('ffi')
local json = require('json')

--
-- Error messages description.
-- * name -- Key by which an error code can be retrieved from
--   the expoted by the module `code` dictionary.
-- * msg -- Error message which can use `args` using
--   `string.format` notation.
-- * args -- Names of arguments passed while constructing an
--   error. After constructed, an error-object contains provided
--   arguments by the names specified in the field.
--
local error_message_template = {
    [1] = {
        name = 'WRONG_BUCKET',
        msg = 'Cannot perform action with bucket %d, reason: %s',
        args = {'bucket_id', 'reason', 'destination'}
    },
    [2] = {
        name = 'NON_MASTER',
        msg = 'Replica %s is not a master for replicaset %s anymore',
        args = {'replica_uuid', 'replicaset_uuid'}
    },
    [3] = {
        name = 'BUCKET_ALREADY_EXISTS',
        msg = 'Bucket %d already exists',
        args = {'bucket_id'}
    },
    [4] = {
        name = 'NO_SUCH_REPLICASET',
        msg = 'Replicaset %s not found',
        args = {'replicaset_uuid'}
    },
    [5] = {
        name = 'MOVE_TO_SELF',
        msg = 'Cannot move: bucket %d is already on replicaset %s',
        args = {'bucket_id', 'replicaset_uuid'}
    },
    [6] = {
         name = 'MISSING_MASTER',
         msg = 'Master is not configured for replicaset %s',
         args = {'replicaset_uuid'}
    },
    [7] = {
        name = 'TRANSFER_IS_IN_PROGRESS',
        msg = 'Bucket %d is transferring to replicaset %s',
        args = {'bucket_id', 'destination'}
    },
    [8] = {
        name = 'UNREACHABLE_REPLICASET',
        msg = 'There is no active replicas in replicaset %s',
        args = {'unreachable_uuid', 'bucket_id'}
    },
    [9] = {
        name = 'NO_ROUTE_TO_BUCKET',
        msg = 'Bucket %d cannot be found. Is rebalancing in progress?',
        args = {'bucket_id'}
    },
    [10] = {
        name = 'NON_EMPTY',
        msg = 'Cluster is already bootstrapped'
    },
    [11] = {
        name = 'UNREACHABLE_MASTER',
        msg = 'Master of replicaset %s is unreachable: %s',
        args = {'uuid', 'reason'}
    },
    [12] = {
        name = 'OUT_OF_SYNC',
        msg = 'Replica is out of sync'
    },
    [13] = {
        name = 'HIGH_REPLICATION_LAG',
        msg = 'High replication lag: %f',
        args = {'lag'}
    },
    [14] = {
        name = 'UNREACHABLE_REPLICA',
        msg = "Replica %s isn't active",
        args = {'unreachable_uuid'}
    },
    [15] = {
        name = 'LOW_REDUNDANCY',
        msg = 'Only one replica is active'
    },
    [16] = {
        name = 'INVALID_REBALANCING',
        msg = 'Sending and receiving buckets at same time is not allowed'
    },
    [17] = {
        name = 'SUBOPTIMAL_REPLICA',
        msg = 'A current read replica in replicaset %s is not optimal'
    },
    [18] = {
        name = 'UNKNOWN_BUCKETS',
        msg = '%d buckets are not discovered',
        args = {'not_discovered_cnt'}
    },
    [19] = {
        name = 'REPLICASET_IS_LOCKED',
        msg = 'Replicaset is locked'
    },
    [20] = {
        name = 'OBJECT_IS_OUTDATED',
        msg = 'Object is outdated after module reload/reconfigure. ' ..
              'Use new instance.'
    },
    [21] = {
        name = 'ROUTER_ALREADY_EXISTS',
        msg = 'Router with name %s already exists',
        args = {'name'},
    },
    [22] = {
        name = 'BUCKET_IS_LOCKED',
        msg = 'Bucket %d is locked',
        args = {'bucket_id'},
    },
    [23] = {
        name = 'INVALID_CFG',
        msg = 'Invalid configuration: %s',
        args = {'reason'},
    },
    [24] = {
        name = 'BUCKET_IS_PINNED',
        msg = 'Bucket %d is pinned',
        args = {'bucket_id'}
    },
    [25] = {
        name = 'TOO_MANY_RECEIVING',
        msg = 'Too many receiving buckets at once, please, throttle'
    },
}

--
-- User-visible error_name -> error_number dictionary.
--
local error_code = {}
for code, err in pairs(error_message_template) do
    assert(type(code) == 'number')
    assert(err.msg, 'msg is required field')
    assert(error_code[err.name] == nil, "Dublicate error name")
    error_code[err.name] = code
end

--
-- There are 2 error types:
-- * box_error - it is created on tarantool errors: client error,
--   oom error, socket error etc. It has type = one of tarantool
--   error types, trace (file, line), message;
-- * vshard_error - it is created on sharding errors like
--   replicaset unavailability, master absense etc. It has type =
--   'ShardingError', one of codes below and optional
--   message.
--

local function box_error(original_error)
    return setmetatable(original_error:unpack(), {__tostring = json.encode})
end

--
-- Construct an vshard error.
-- @param code Number, one of error_code constants.
-- @param ... Arguments from `error_message_template` `args`
--        field. Caller have to pass at least as many arguments
--        as `msg` field requires.
-- @retval ShardingError object.
--
local function vshard_error(code, ...)
    local format = error_message_template[code]
    assert(format, 'Error message format is not found.')
    local args_passed_cnt = select('#', ...)
    local args = format.args or {}
    assert(#args == args_passed_cnt,
           string.format('Wrong number of arguments are passed to %s error',
                         format.name))
    local ret = setmetatable({}, {__tostring = json.encode})
    -- Save error arguments.
    for i = 1, #args do
        ret[args[i]] = select(i, ...)
    end
    ret.message = string.format(format.msg, ...)
    ret.type = 'ShardingError'
    ret.code = code
    ret.name = format.name
    return ret
end

--
-- Convert error object from pcall to lua, box or vshard error
-- object.
--
local function make_error(e)
    if type(e) == 'cdata' and ffi.istype('struct error', e) then
        -- box.error, return unpacked
        return box_error(e)
    elseif type(e) == 'string' then
        local ok, err = pcall(box.error, box.error.PROC_LUA, e)
        return box_error(err)
    elseif type(e) == 'table' then
        return setmetatable(e, {__tostring = json.encode})
    else
        return e
    end
end


local function make_alert(code, ...)
    local format = error_message_template[code]
    assert(format)
    local r = {format.name, string.format(format.msg, ...)}
    return setmetatable(r, { __serialize = 'seq' })
end

return {
    code = error_code,
    box = box_error,
    vshard = vshard_error,
    make = make_error,
    alert = make_alert,
}
