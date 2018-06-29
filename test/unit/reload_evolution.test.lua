test_run = require('test_run').new()
fiber = require('fiber')
log = require('log')
util = require('util')
reload_evolution = require('vshard.storage.reload_evolution')
-- Init with the latest version.
fake_M = { reload_version = reload_evolution.version }

-- Test reload to the same version.
reload_evolution.upgrade(fake_M)
test_run:grep_log('default', 'vshard.storage.evolution') == nil

-- Test downgrage version.
log.info(string.rep('a', 1000))
fake_M.reload_version = fake_M.reload_version + 1
err = util.check_error(reload_evolution.upgrade, fake_M)
err:match('auto%-downgrade is not implemented')
test_run:grep_log('default', 'vshard.storage.evolution', 1000) ~= nil
