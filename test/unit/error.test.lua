test_run = require('test_run').new()
vshard = require('vshard')
util = require('util')
lerror = vshard.error

--
-- Test string representations of errors.
--
ok, err = pcall(box.error, box.error.TIMEOUT)
box_error = lerror.box(err)
tostring(box_error)

vshard_error = lerror.vshard(lerror.code.UNREACHABLE_MASTER, 'uuid', 'reason')
tostring(vshard_error)

log = require('log')
log.info('Log error: %s', vshard_error)
test_run:grep_log('default', '"reason":"reason","code":11,"type":"ShardingError"')

--
-- Part of gh-100: check `error.vshard`.
--
lerror.vshard(lerror.code.WRONG_BUCKET, 1, 'arg2', 'arg3')
-- Pass an arg of a wrong type.
util.check_error(lerror.vshard, lerror.code.WRONG_BUCKET, 'arg1', 'arg2', 100)
-- Pass less args than msg requires.
util.check_error(lerror.vshard, lerror.code.MISSING_MASTER)
-- Pass more args than `args` field contains.
util.check_error(lerror.vshard, lerror.code.MISSING_MASTER, 'arg1', 'arg2')
-- Pass wrong format code.
util.check_error(lerror.vshard, 'Wrong format code', 'arg1', 'arg2')

function raise_lua_err() assert(false) end
ok, err = pcall(raise_lua_err)
lerror.make(err)
