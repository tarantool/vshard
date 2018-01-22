#!/usr/bin/env tarantool
require('strict').on()
cfg = require('config')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')
local names = require('names')
rs_uuid = names.rs_uuid
replica_uuid = names.replica_uuid

box.cfg{listen = 3333}
vshard.router.cfg(cfg)

require('console').listen(os.getenv('ADMIN'))
