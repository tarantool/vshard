#!/usr/bin/env tarantool
cfg = dofile('config.lua')
vshard = require('vshard')
os = require('os')
fiber = require('fiber')

box.cfg{}
vshard.router.cfg(cfg)

require('console').listen(os.getenv('ADMIN'))
