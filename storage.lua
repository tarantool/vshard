#!/usr/bin/env tarantool

-- Get instance name
local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

-- Call a configuration provider
local cfg = require('devcfg')

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, NAME)
