#!/usr/bin/env tarantool
cfg = dofile('config.lua')
cfg.listen = 3300
require('console').listen(os.getenv('ADMIN'))
vshard = require('vshard')
require('util').box_router_cfg(cfg)
