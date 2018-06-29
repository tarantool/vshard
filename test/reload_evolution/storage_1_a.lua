#!/usr/bin/env tarantool

require('strict').on()

local log = require('log')
local fiber = require('fiber')
local util = require('lua_libs.util')
local fio = require('fio')

-- Get instance name
NAME = fio.basename(arg[0], '.lua')

-- test-run gate.
test_run = require('test_run').new()
require('console').listen(os.getenv('ADMIN'))

-- Run one storage on a different vshard version.
-- To do that, place vshard src to
-- BUILDDIR/test/var/vshard_git_tree_copy/.
if NAME == 'storage_2_a' then
    local script_path = debug.getinfo(1).source:match("@?(.*/)")
    vshard_copy = util.BUILDDIR .. '/test/var/vshard_git_tree_copy'
    package.path = string.format(
        '%s/?.lua;%s/?/init.lua;%s',
        vshard_copy, vshard_copy, package.path
    )
end

-- Call a configuration provider
cfg = require('localcfg')
-- Name to uuid map
names = {
    ['storage_1_a'] = '8a274925-a26d-47fc-9e1b-af88ce939412',
    ['storage_1_b'] = '3de2e3e1-9ebe-4d0d-abb1-26d301b84633',
    ['storage_2_a'] = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    ['storage_2_b'] = '001688c3-66f8-4a31-8e19-036c17d489c2',
}

replicaset1_uuid = 'cbf06940-0790-498b-948d-042b62cf3d29'
replicaset2_uuid = 'ac522f65-aa94-4134-9f64-51ee384f1a54'
replicasets = {replicaset1_uuid, replicaset2_uuid}

-- Start the database with sharding
vshard = require('vshard')
vshard.storage.cfg(cfg, names[NAME])

-- Bootstrap storage.
require('lua_libs.bootstrap')
