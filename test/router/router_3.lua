#!/usr/bin/env tarantool
fiber = require('fiber')
cfg = dofile('config.lua')
cfg.listen = 3300
require('console').listen(os.getenv('ADMIN'))
vshard = require('vshard')
box.cfg{listen = cfg.listen}
