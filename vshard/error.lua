local ffi = require('ffi')
local json = require('json')

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

local function vshard_error(code, args, msg)
    local ret = setmetatable({type = 'ShardingError', code = code,
                              message = msg}, {__tostring = json.encode})
    for k, v in pairs(args) do
        ret[k] = v
    end
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
    else
        return e
    end
end

local error_code = {
    -- Error codes. Some of them are used for alerts too.
    WRONG_BUCKET = 1,
    NON_MASTER = 2,
    BUCKET_ALREADY_EXISTS = 3,
    NO_SUCH_REPLICASET = 4,
    MOVE_TO_SELF = 5,
    MISSING_MASTER = 6,
    TRANSFER_IS_IN_PROGRESS = 7,
    UNREACHABLE_REPLICASET = 8,
    NO_ROUTE_TO_BUCKET = 9,
    NON_EMPTY = 10,

    -- Alert codes.
    UNREACHABLE_MASTER = 11,
    OUT_OF_SYNC = 12,
    HIGH_REPLICATION_LAG = 13,
    UNREACHABLE_REPLICA = 14,
    LOW_REDUNDANCY = 15,
    INVALID_REBALANCING = 16,
    SUBOPTIMAL_REPLICA = 17,
    UNKNOWN_BUCKETS = 18,
}

local error_message_template = {
    [error_code.MISSING_MASTER] = {
         name = 'MISSING_MASTER',
         msg = 'Master is not configured for replicaset %s'
    },
    [error_code.UNREACHABLE_MASTER] = {
        name = 'UNREACHABLE_MASTER',
        msg = 'Master of replicaset %s is unreachable: %s'
    },
    [error_code.OUT_OF_SYNC] = {
        name = 'OUT_OF_SYNC',
        msg = 'Replica is out of sync'
    },
    [error_code.HIGH_REPLICATION_LAG] = {
        name = 'HIGH_REPLICATION_LAG',
        msg = 'High replication lag: %f'
    },
    [error_code.UNREACHABLE_REPLICA] = {
        name = 'UNREACHABLE_REPLICA',
        msg = "Replica %s isn't active"
    },
    [error_code.UNREACHABLE_REPLICASET] = {
        name = 'UNREACHABLE_REPLICASET',
        msg = 'There is no active replicas in replicaset %s'
    },
    [error_code.LOW_REDUNDANCY] = {
        name = 'LOW_REDUNDANCY',
        msg = 'Only one replica is active'
    },
    [error_code.INVALID_REBALANCING] = {
        name = 'INVALID_REBALANCING',
        msg = 'Sending and receiving buckets at same time is not allowed'
    },
    [error_code.SUBOPTIMAL_REPLICA] = {
        name = 'SUBOPTIMAL_REPLICA',
        msg = 'A current read replica in replicaset %s is not optimal'
    },
    [error_code.UNKNOWN_BUCKETS] = {
        name = 'UNKNOWN_BUCKETS',
        msg = '%d buckets are not discovered',
    }
}

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
