#!/usr/bin/env tarantool

require('strict').on()

-- Get instance name
local fio = require('fio')
local NAME = fio.basename(arg[0], '.lua')

require('console').listen(os.getenv('ADMIN'))

configs = require('configs')

-- Start the database with sharding
vshard = require('vshard')
box.cfg{}
