#!/usr/bin/env tarantool
fiber = require('fiber')
cfg = dofile('config.lua')
require('console').listen(os.getenv('ADMIN'))
vshard = require('vshard')
box.cfg{}
