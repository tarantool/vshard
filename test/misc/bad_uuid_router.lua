#!/usr/bin/env tarantool

test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

-- Call a configuration provider
cfg = require('bad_uuid_config').cfg

-- Start the database with sharding
vshard = require('vshard')
util = require('util')
vshard.router.cfg(cfg)
box.cfg{listen = 3300}
