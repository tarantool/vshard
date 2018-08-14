#!/usr/bin/env tarantool

-- Get instance name.
NAME = require('fio').basename(arg[0], '.lua')

-- Fetch config for the cluster of the instance.
if NAME:sub(9,9) == '1' then
    cfg = require('configs').cfg_1
else
    cfg = require('configs').cfg_2
end
require('storage_template')
