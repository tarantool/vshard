test_run = require('test_run').new()
vshard = require('vshard')
lerror = vshard.error

--
-- Test string representations of errors.
--
ok, err = pcall(box.error, box.error.TIMEOUT)
box_error = lerror.box(err)
tostring(box_error)

vshard_error = lerror.vshard(lerror.code.UNREACHABLE_MASTER, {uuid = 'uuid', reason = 'reason'}, 'message 1 2 3')
tostring(vshard_error)

log = require('log')
log.info('Log error: %s', vshard_error)
test_run:grep_log('default', '"reason":"reason","code":11,"type":"ShardingError"')

function raise_lua_err() assert(false) end
ok, err = pcall(raise_lua_err)
lerror.make(err)
