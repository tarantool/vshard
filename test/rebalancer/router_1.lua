#!/usr/bin/env tarantool
cfg = require('config')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')

box.cfg{listen = 3333}
vshard.router.cfg(cfg)

require('console').listen(os.getenv('ADMIN'))
