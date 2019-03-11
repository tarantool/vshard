#!/usr/bin/env tarantool
cfg = require('config')
cfg.listen = 3300
require('console').listen(os.getenv('ADMIN'))
vshard = require('vshard')
vshard.router.cfg(cfg)
