#!/usr/bin/env tarantool
cfg = dofile('config.lua')
require('console').listen(os.getenv('ADMIN'))
vshard = require('vshard')
vshard.router.cfg(cfg)
