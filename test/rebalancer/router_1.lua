#!/usr/bin/env tarantool
cfg = dofile('config.lua')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')
util = require('util')

box.cfg{listen = 3333}
util.box_router_cfg(cfg)

require('console').listen(os.getenv('ADMIN'))
