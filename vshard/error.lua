local ffi = require('ffi')
local json = require('json')

--
-- Error messages description.
-- * name -- Key by which an error code can be retrieved from
--   the exported by the module `code` dictionary.
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
        args = {'replica', 'replicaset', 'master'}
    },
    [3] = {
        name = 'BUCKET_ALREADY_EXISTS',
        msg = 'Bucket %d already exists',
        args = {'bucket_id'}
    },
    [4] = {
        name = 'NO_SUCH_REPLICASET',
        msg = 'Replicaset %s not found',
        args = {'replicaset'}
    },
    [5] = {
        name = 'MOVE_TO_SELF',
        msg = 'Cannot move: bucket %d is already on replicaset %s',
        args = {'bucket_id', 'replicaset'}
    },
    [6] = {
         name = 'MISSING_MASTER',
         msg = 'Master is not configured for replicaset %s',
         args = {'replicaset'}
    },
    [7] = {
        name = 'TRANSFER_IS_IN_PROGRESS',
        msg = 'Bucket %d is transferring to replicaset %s',
        args = {'bucket_id', 'destination'}
    },
    [8] = {
        name = 'UNREACHABLE_REPLICASET',
        msg = 'There is no active replicas in replicaset %s',
        args = {'replicaset', 'bucket_id'}
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
        args = {'replicaset', 'reason'}
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
        args = {'replica'}
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
        args = {'router_name'},
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
    [26] = {
        name = 'STORAGE_IS_REFERENCED',
        msg = 'Storage is referenced'
    },
    [27] = {
        name = 'STORAGE_REF_ADD',
        msg = 'Can not add a storage ref: %s',
        args = {'reason'},
    },
    [28] = {
        name = 'STORAGE_REF_USE',
        msg = 'Can not use a storage ref: %s',
        args = {'reason'},
    },
    [29] = {
        name = 'STORAGE_REF_DEL',
        msg = 'Can not delete a storage ref: %s',
        args = {'reason'},
    },
    [30] = {
        name = 'BUCKET_RECV_DATA_ERROR',
        msg = 'Can not receive the bucket %s data in space "%s" at tuple %s: %s',
        args = {'bucket_id', 'space', 'tuple', 'reason'},
    },
    [31] = {
        name = 'MULTIPLE_MASTERS_FOUND',
        msg = 'Found more than one master in replicaset %s on nodes %s and %s',
        args = {'replicaset', 'master1', 'master2'},
    },
    [32] = {
        name = 'REPLICASET_IN_BACKOFF',
        msg = 'Replicaset %s is in backoff, can\'t take requests right now. '..
              'Last error was %s',
        args = {'replicaset', 'error'}
    },
    [33] = {
        name = 'STORAGE_IS_DISABLED',
        msg = 'Storage is disabled: %s',
        args = {'reason'}
    },
    [34] = {
        -- That is similar to WRONG_BUCKET, but the latter is not critical. It
        -- usually can be retried. Corruption is a critical error, it requires
        -- more attention.
        name = 'BUCKET_IS_CORRUPTED',
        msg = 'Bucket %d is corrupted: %s',
        args = {'bucket_id', 'reason'}
    },
    [35] = {
        name = 'ROUTER_IS_DISABLED',
        msg = 'Router is disabled: %s',
        args = {'reason'}
    },
    [36] = {
        name = 'BUCKET_GC_ERROR',
        msg = 'Error during bucket GC: %s',
        args = {'reason'},
    },
    [37] = {
        name = 'STORAGE_CFG_IS_IN_PROGRESS',
        msg = 'Configuration of the storage is in progress',
    },
    [38] = {
        name = 'ROUTER_CFG_IS_IN_PROGRESS',
        msg = 'Configuration of the router with name %s is in progress',
        args = {'router_name'}
    },
    [39] = {
        name = 'BUCKET_INVALID_UPDATE',
        msg = 'Bucket %s update is invalid: %s',
        args = {'bucket_id', 'reason'},
    },
    [40] = {
        name = 'VHANDSHAKE_NOT_COMPLETE',
        msg = 'Handshake with %s have not been completed yet',
        args = {'replica'},
    },
    [41] = {
        name = 'INSTANCE_NAME_MISMATCH',
        msg = 'Mismatch server name: expected "%s", but got "%s"',
        args = {'expected_name', 'actual_name'},
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
--   replicaset unavailability, master absence etc. It has type =
--   'ShardingError', one of codes below and optional
--   message.
--
local box_error_mt = {__tostring = json.encode}
local function box_error(original_error)
    local res = setmetatable(original_error:unpack(), box_error_mt)
    local pos = res
    local prev = pos.prev
    while prev ~= nil do
        prev = setmetatable(prev:unpack(), box_error_mt)
        pos.prev = prev
        pos = prev
        prev = pos.prev
    end
    return res
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
        return box_error(box.error.new(box.error.PROC_LUA, e))
    elseif type(e) == 'table' then
        return setmetatable(e, {__tostring = json.encode})
    else
        return e
    end
end

--
-- Restore an error object from its string serialization.
--
local function from_string(str)
    -- Error objects in VShard are stringified into json. Hence can restore also
    -- as json. The only problem is that the json might be truncated if it was
    -- stored in an error message of a real error object. It is not very
    -- reliable.
    local ok, res = pcall(json.decode, str)
    if not ok then
        return nil
    end
    if type(res) ~= 'table' or type(res.type) ~= 'string' or
       type(res.code) ~= 'number' or type(res.message) ~= 'string' then
        return nil
    end
    return make_error(res)
end

local function make_alert(code, ...)
    local format = error_message_template[code]
    assert(format)
    local r = {format.name, string.format(format.msg, ...)}
    return setmetatable(r, { __serialize = 'seq' })
end

--
-- Create a timeout error object.
--
local function make_timeout()
    return make_error(box.error.new(box.error.TIMEOUT))
end

--
-- Timeout error can be a ClientError with ER_TIMEOUT code or a TimedOut error
-- which is ER_SYSTEM. They also have different messages. Same public APIs can
-- return both errors depending on core version and/or error cause. This func
-- helps not to care.
--
local function error_is_timeout(err)
    return err.code == box.error.TIMEOUT or (err.code == box.error.PROC_LUA and
           err.message == 'Timeout exceeded') or err.type == 'TimedOut'
end

return {
    code = error_code,
    box = box_error,
    vshard = vshard_error,
    make = make_error,
    from_string = from_string,
    alert = make_alert,
    timeout = make_timeout,
    is_timeout = error_is_timeout,
}
