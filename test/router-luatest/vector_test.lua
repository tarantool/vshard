local t = require('luatest')
local vtest = require('test.luatest_helpers.vtest')

local g = t.group('vector')

local cfg_template = {
    sharding = {
        {
            replicas = {
                replica_1_a = {master = true},
            },
        },
        {
            replicas = {
                replica_2_a = {master = true},
            },
        },
    },
    bucket_count = 20,
    test_user_grant_range = 'super',
}

local function vector_cfg()
    return {
        collections = {
            products = {
                collection_id = 1,
                generation = 1,
                space = 'products',
                index = 'embedding',
                primary_index = 'primary',
                bucket_field = 'bucket_id',
                version_field = 'vector_version',
                dimension = 2,
                distance = 'l2',
            },
        },
    }
end

g.before_all(function(cg)
    cg.cfg = vtest.config_new(cfg_template)
    vtest.cluster_new(cg, cg.cfg)
    cg.router = vtest.router_new(cg, 'router', cg.cfg)
    local ok, err = cg.router:exec(function()
        return ivshard.router.bootstrap({timeout = iwait_timeout})
    end)
    t.assert(ok, tostring(err))

    vtest.cluster_exec_each_master(cg, function(config)
        local space = box.schema.space.create('products', {
            format = {
                {name = 'id', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'vector_version', type = 'unsigned'},
                {name = 'embedding', type = 'array'},
                {name = 'title', type = 'string'},
            },
        })
        space:create_index('primary')
        space:create_index('bucket_id', {
            unique = false,
            parts = {{field = 'bucket_id', type = 'unsigned'}},
        })
        space:create_index('embedding', {
            type = 'vector',
            unique = false,
            parts = {{field = 'embedding', type = 'array'}},
            dimension = 2,
            distance = 'l2',
            m = 8,
            ef_construction = 32,
            ef_search = 32,
        })
        require('vshard.vector').cfg(config)
    end, {vector_cfg()})
    cg.router:exec(function(config)
        require('vshard.vector').cfg(config)
    end, {vector_cfg()})

    local bucket_1 = vtest.storage_first_bucket(cg.replica_1_a)
    local bucket_2 = vtest.storage_first_bucket(cg.replica_2_a)
    cg.replica_1_a:exec(function(bucket_id)
        box.space.products:insert({1, bucket_id, 1, {0, 0}, 'first'})
    end, {bucket_1})
    cg.replica_2_a:exec(function(bucket_id)
        box.space.products:insert({2, bucket_id, 1, {1, 0}, 'second'})
    end, {bucket_2})
end)

g.after_all(function(cg)
    cg.cluster:drop()
end)

g.test_search = function(cg)
    local rows, meta = cg.router:exec(function()
        return require('vshard.vector').search('products', {0, 0}, {
            k = 2,
            local_k = 2,
            timeout = iwait_timeout,
            return_fields = {'id', 'title'},
        })
    end)
    t.assert(meta.complete)
    t.assert_equals(meta.shards_total, 2)
    t.assert_equals({rows[1].key, rows[2].key}, {1, 2})
    t.assert_equals(rows[1].tuple, {1, 'first'})
end
