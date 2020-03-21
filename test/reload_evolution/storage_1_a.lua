#!/usr/bin/env tarantool
local util = require('util')
NAME = require('fio').basename(arg[0], '.lua')

-- Run one storage on a different vshard version.
-- To do that, place vshard src to
-- BUILDDIR/test/var/vshard_git_tree_copy/.
original_package_path = package.path
if NAME == 'storage_2_a' then
    vshard_copy = util.BUILDDIR .. '/test/var/vshard_git_tree_copy'
    package.path = string.format(
        '%s/?.lua;%s/?/init.lua;%s',
        vshard_copy, vshard_copy, original_package_path
    )
end
require('storage_template')
