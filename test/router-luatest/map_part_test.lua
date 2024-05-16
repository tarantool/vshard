
local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('router')
local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {
                    master = true,
                },
                replica_1_b = {},
            },
        },
        {
            replicas = {
                replica_2_a = {
                    master = true,
                },
                replica_2_b = {},
            },
        },
    },
    bucket_count = 100,
    test_user_grant_range = 'super',
}
local global_cfg = vtest.config_new(cfg_template)

g.before_all(function()
    vtest.cluster_new(g, global_cfg)

    t.assert_equals(g.replica_1_a:exec(function()
        return #ivshard.storage.info().alerts
    end), 0, 'no alerts after boot')

    local router = vtest.router_new(g, 'router', global_cfg)
    g.router = router
    local res, err = router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(res and not err, 'bootstrap buckets')
end)

g.after_all(function()
    g.cluster:drop()
end)

local function map_part_init()
    local rs1_uuid = g.replica_1_a:replicaset_uuid()
    local rs2_uuid = g.replica_2_a:replicaset_uuid()

    local create_map_func_f = function(res1)
        rawset(_G, 'do_map', function(res2)
            return {res1, res2}
        end)
    end
    g.replica_1_a:exec(create_map_func_f, {1})
    g.replica_2_a:exec(create_map_func_f, {2})

    local bids1 = vtest.storage_get_n_buckets(g.replica_1_a, 4)
    local bids2 = vtest.storage_get_n_buckets(g.replica_2_a, 1)

    return {
        rs1_uuid = rs1_uuid,
        rs2_uuid = rs2_uuid,
        bids1 = bids1,
        bids2 = bids2,
    }
end

g.test_map_part_single_rs = function(g)
    local expected, res
    local init = map_part_init()

    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1, bid2},
            'do_map',
            {3},
            {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[3], init.bids1[2]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    expected = {
        [init.rs1_uuid] = {{1, 3}},
    }
    t.assert_equals(res.val, expected)
end

g.test_map_part_multi_rs = function(g)
    local expected, res
    local init = map_part_init()

    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw({bid1, bid2}, 'do_map', {42},
            {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[1], init.bids2[1]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    expected = {
        [init.rs1_uuid] = {{1, 42}},
        [init.rs2_uuid] = {{2, 42}},
    }
    t.assert_equals(res.val, expected)
end

g.test_map_part_ref = function(g)
    local expected, res
    local init = map_part_init()

    -- First move some buckets from rs1 to rs2 and then pause gc on rs1.
    -- As a result, the buckets will be in the SENT state on rs1 and
    -- in the ACTIVE state on rs2.
    g.replica_1_a:exec(function(bid1, bid2, to)
        ivshard.storage.internal.errinj.ERRINJ_BUCKET_GC_PAUSE = true
        ivshard.storage.bucket_send(bid1, to)
        ivshard.storage.bucket_send(bid2, to)
    end, {init.bids1[1], init.bids1[2], init.rs2_uuid})
    -- The buckets are ACTIVE on rs2, so the partial map should succeed.
    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1, bid2}, 'do_map', {42}, {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[1], init.bids1[2]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    expected = {
        [init.rs2_uuid] = {{2, 42}},
    }
    t.assert_equals(res.val, expected)
    -- But if we use some active bucket from rs1, the partial map should fail.
    -- The reason is that the moved buckets are still in the SENT state and
    -- we can't take a ref.
    res = g.router:exec(function(bid1)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1}, 'do_map', {42}, {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[3]})
    t.assert_equals(res.val, nil)
    t.assert(res.err)
    t.assert_equals(res.err_uuid, init.rs1_uuid)
    -- The moved buckets still exist on the rs1 with non-active status.
    -- Let's remove them and re-enable gc on rs1.
    g.replica_1_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_BUCKET_GC_PAUSE = false
        _G.bucket_gc_wait()
    end)
    -- Now move the buckets back to rs1 and pause gc on rs2.
    -- The buckets will be ACTIVE on rs1 and SENT on rs2,
    -- so the partial map should succeed.
    res = g.replica_2_a:exec(function(bid1, bid2, to)
        ivshard.storage.internal.errinj.ERRINJ_BUCKET_GC_PAUSE = true
        ivshard.storage.bucket_send(bid1, to)
        local res, err = ivshard.storage.bucket_send(bid2, to)
        return {
            res = res,
            err = err,
        }
    end, {init.bids1[1], init.bids1[2], init.rs1_uuid})
    t.assert_equals(res.err, nil)

    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1, bid2}, 'do_map', {42}, {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[1], init.bids1[2]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    expected = {
        [init.rs1_uuid] = {{1, 42}},
    }
    t.assert_equals(res.val, expected)
    -- Re-enable gc on rs2.
    g.replica_2_a:exec(function()
        ivshard.storage.internal.errinj.ERRINJ_BUCKET_GC_PAUSE = false
        _G.bucket_gc_wait()
    end)
end

g.test_map_part_double_ref = function(g)
    local expected, res
    local init = map_part_init()

    -- First, disable discovery on the router to disable route cache update.
    g.router:exec(function()
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = true
    end)
    -- Then, move the bucket form rs1 to rs2. Now the router has an outdated
    -- route cache.
    g.replica_1_a:exec(function(bid, to)
        ivshard.storage.bucket_send(bid, to)
    end, {init.bids1[4], init.rs2_uuid})
    -- Call a partial map for the moved bucket and some bucket from rs2. The ref stage
    -- should be done in two steps:
    -- 1. ref rs2 and returns the moved bucket;
    -- 2. discover the moved bucket on rs2 and avoid double reference;
    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1, bid2}, 'do_map', {42}, {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[4], init.bids2[1]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    expected = {
        [init.rs2_uuid] = {{2, 42}},
    }
    t.assert_equals(res.val, expected)
    -- Call a partial map one more time to make sure there are no references left.
    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw(
            {bid1, bid2}, 'do_map', {42}, {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[4], init.bids2[1]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, expected)
    -- Return the bucket back and re-enable discovery on the router.
    g.replica_2_a:exec(function(bid, to)
        ivshard.storage.bucket_send(bid, to)
    end, {init.bids1[4], init.rs1_uuid})
    g.router:exec(function()
        ivshard.router.internal.errinj.ERRINJ_LONG_DISCOVERY = false
    end)
end

g.test_map_part_map = function(g)
    local res
    local init = map_part_init()

    g.replica_2_a:exec(function()
        _G.do_map = function()
            return box.error(box.error.PROC_LUA, "map_err")
        end
    end)
    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw({bid2, bid1}, 'do_map', {3},
            {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[1], init.bids2[1]})
    t.assert_equals(res.val, nil)
    t.assert_covers(res.err, {
        code = box.error.PROC_LUA,
        type = 'ClientError',
        message = 'map_err'
    })
    t.assert_equals(res.err_uuid, init.rs2_uuid)
    -- Check that there is no dangling references after the error.
    init = map_part_init()
    res = g.router:exec(function(bid1, bid2)
        local val, err, err_uuid = ivshard.router.map_part_callrw({bid1, bid2}, 'do_map', {3},
            {timeout = iwait_timeout})
        return {
            val = val,
            err = err,
            err_uuid = err_uuid,
        }
    end, {init.bids1[1], init.bids2[1]})
    t.assert_equals(res.err, nil)
    t.assert_equals(res.err_uuid, nil)
    t.assert_equals(res.val, {
        [init.rs1_uuid] = {{1, 3}},
        [init.rs2_uuid] = {{2, 3}},
    })
end
