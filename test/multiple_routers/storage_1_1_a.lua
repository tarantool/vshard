#!/usr/bin/env tarantool

require('strict').on()

-- Get instance name.
local fio = require('fio')
NAME = fio.basename(arg[0], '.lua')

require('console').listen(os.getenv('ADMIN'))

-- Fetch config for the cluster of the instance.
if NAME:sub(9,9) == '1' then
    cfg = require('configs').cfg_1
else
    cfg = require('configs').cfg_2
end

-- Start the database with sharding.
vshard = require('vshard')
vshard.storage.cfg(cfg, names[NAME])

-- Bootstrap storage.
require('lua_libs.bootstrap')
