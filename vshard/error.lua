local ffi = require('ffi')

--
-- There are 3 error types:
-- * lua error - it is created on assertions, syntax errors,
--   luajit OOM etc. It has attributes type = 'LuajitError' and
--   optional message;
-- * box_error - it is created on tarantool errors: client error,
--   oom error, socket error etc. It has type = one of tarantool
--   error types, trace (file, line), message;
-- * vshard_error - it is created on sharding errors like
--   replicaset unavailability, master absense etc. It has type =
--   'ShardingError', one of codes below and optional
--   message.
--
local function lua_error(msg)
    return { type = 'LuajitError', message = msg }
end

local function box_error(original_error)
    return original_error:unpack()
end

local function vshard_error(code, args, msg)
    local ret = { type = 'ShardingError', code = code, message = msg }
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
   if type(e) == 'table' then
       -- Custom error object, return as is
       return e
   elseif type(e) == 'cdata' and ffi.istype('struct error', e) then
       -- box.error, return unpacked
       return box_error(e)
   else
       -- Lua error, return wrapped
       return lua_error(tostring(e))
   end
end

return {
    code = {
        WRONG_BUCKET = 0x01,
        NON_MASTER = 0x02,
        BUCKET_ALREADY_EXISTS = 0x03,
        NO_SUCH_REPLICASET = 0x04,
        MOVE_TO_SELF = 0x05,
        MISSING_MASTER = 0x06,
        TRANSFER_IS_IN_PROGRESS = 0x07,
        REPLICASET_IS_UNREACHABLE = 0x08,
        NO_ROUTE_TO_BUCKET = 0x09,
    },
    lua = lua_error,
    box = box_error,
    vshard = vshard_error,
    make = make_error,
}
