#!/usr/bin/env tarantool
local util = require('util')
NAME = require('fio').basename(arg[0], '.lua')

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
require('storage_template')
