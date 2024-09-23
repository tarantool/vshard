#!/usr/bin/env tarantool
local util = require('util')
NAME = require('fio').basename(arg[0], '.lua')
local source_path = arg[1]
original_package_path = package.path
if NAME == 'storage_2_a' then
    package.path = string.format('%s/?.lua;%s/?/init.lua;%s', source_path,
                                 source_path, package.path)
end
require('storage_template')
