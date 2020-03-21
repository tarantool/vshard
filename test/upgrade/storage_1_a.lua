#!/usr/bin/env tarantool
util = require('util')
NAME = require('fio').basename(arg[0], '.lua')
local source_path = arg[1]
if source_path then
    -- Run one storage on a different vshard
    -- version.
    package.path = string.format('%s/?.lua;%s/?/init.lua;%s', source_path,
                                 source_path, package.path)
end
require('storage_template')
