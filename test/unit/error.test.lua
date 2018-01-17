test_run = require('test_run').new()
vshard = require('vshard')
lerror = vshard.error

--
-- Test string representations of errors.
--

lua_error = lerror.lua('asserion failed')
tostring(lua_error)

ok, err = pcall(box.error, box.error.TIMEOUT)
box_error = lerror.box(err)
tostring(box_error)

vshard_error = lerror.vshard(lerror.code.UNREACHABLE_MASTER, {uuid = 'uuid', reason = 'reason'}, 'message 1 2 3')
tostring(vshard_error)

log = require('log')
log.info('Log error: %s', vshard_error)
test_run:grep_log('default', '"reason":"reason","code":11,"type":"ShardingError"')
