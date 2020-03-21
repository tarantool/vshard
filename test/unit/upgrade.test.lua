test_run = require('test_run').new()

-- There is no way to revert a bootstrapped storage manually. The
-- test is done in a new instance which is killed with all its
-- data afterwards.

_ = test_run:cmd("create server newdefault with script='unit/box2.lua'")
_ = test_run:cmd("start server newdefault")
_ = test_run:switch("newdefault")

util = require('util')

vshard = require('vshard')

upgrade = vshard.storage.internal.schema_upgrade_master
handlers = vshard.storage.internal.schema_upgrade_handlers
version_make = vshard.storage.internal.schema_version_make
curr_version = vshard.storage.internal.schema_current_version
schema_bootstrap = vshard.storage.internal.schema_bootstrap

user = 'storage'
password = 'storage'

schema_bootstrap(user, password)

version_make({0, 1, 16, 0})

versions = {}

_ = test_run:cmd("setopt delimiter ';'")
for v1 = 1, 3 do
    for v2 = 1, 3 do
        for v3 = 1, 3 do
            for v4 = 1, 3 do
                table.insert(versions, version_make({v1, v2, v3, v4}))
            end
        end
    end
end;

err = nil;
count = 0;
for i, v1 in pairs(versions) do
    for j, v2 in pairs(versions) do
        local v1n = string.format("%s.%s.%s.%s", v1[1], v1[2], v1[3], v1[4])
        local v2n = string.format("%s.%s.%s.%s", v2[1], v2[2], v2[3], v2[4])
        count = count + 1
        if ((v1 == v2) ~= (v1n == v2n)) or ((v1 ~= v2) ~= (v1n ~= v2n)) or
            ((v1 <= v2) ~= (v1n <= v2n)) or ((v1 < v2) ~= (v1n < v2n)) or
            ((v1 >= v2) ~= (v1n >= v2n)) or ((v1 > v2) ~= (v1n > v2n)) then
            err = {i, j}
            break
        end
    end
    if err then
        break
    end
end;
err;
count;

function stat_collect()
    return {
        func_count = box.space._func:count(),
        space_count = box.space._space:count(),
        user_count = box.space._user:count(),
        schema_count = box.space._schema:count(),
        version = curr_version(),
    }
end;

function stat_diff(s1, s2)
    local res = {}
    if s2.func_count ~= s1.func_count then
        res.func_count = s2.func_count - s1.func_count
    end
    if s2.space_count ~= s1.space_count then
        res.space_count = s2.space_count - s1.space_count
    end
    if s2.user_count ~= s1.user_count then
        res.user_count = s2.user_count - s1.user_count
    end
    if s2.schema_count ~= s1.schema_count then
        res.schema_count = s2.schema_count - s1.schema_count
    end
    if s1.version ~= s2.version then
        res.old_version = s1.version
        res.new_version = s2.version
    end
    return res
end;

stat = stat_collect();

function stat_update()
    local new_stat = stat_collect()
    local diff = stat_diff(stat, new_stat)
    stat = new_stat
    return diff
end;

upgrade_trace = {};
errinj = vshard.storage.internal.errinj;

for _, handler in pairs(handlers) do
    table.insert(upgrade_trace, string.format('Upgrade to %s', handler.version))

    table.insert(upgrade_trace, 'Errinj in begin')
    errinj.ERRINJ_UPGRADE = 'begin'
    table.insert(upgrade_trace,
                 {util.check_error(upgrade, handler.version, user, password)})
    table.insert(upgrade_trace, {diff = stat_update()})

    table.insert(upgrade_trace, 'Errinj in end')
    errinj.ERRINJ_UPGRADE = 'end'
    table.insert(upgrade_trace,
                 {util.check_error(upgrade, handler.version, user, password)})
    table.insert(upgrade_trace, {diff = stat_update()})

    table.insert(upgrade_trace, 'No errinj')
    errinj.ERRINJ_UPGRADE = nil
    table.insert(upgrade_trace,
                 {pcall(upgrade, handler.version, user, password)})
    table.insert(upgrade_trace, {diff = stat_update()})
end;

_ = test_run:cmd("setopt delimiter ''");

upgrade_trace

_ = test_run:switch("default")
_ = test_run:cmd("stop server newdefault")
_ = test_run:cmd("cleanup server newdefault")
_ = test_run:cmd("delete server newdefault")
