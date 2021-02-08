local log = require('log')
netbox = require('net.box')

--box.cfg{listen = 3301}
box.cfg{}
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})
box.schema.func.create('routerc.router_callrw', {language = "C", if_not_exists = true})
box.schema.func.create('routerc.router_cfg', {language = "C", if_not_exists = true})
--log.info('Call %s', box.func['routerc.router_callrw']:call())

config =  {
    sharding = {
        ['cbf06940-0790-498b-948d-042b62cf3d29'] = {
            replicas = {
                ['8a274925-a26d-47fc-9e1b-af88ce939412'] = {
                    uri = '127.0.0.1:3301',
                    name = 'storage_1_a',
                    master = true
                },
                ['8a274925-a26d-47fc-9e1b-af88ce939413'] = {
                    uri = '127.0.0.1:3301',
                    name = 'storage_1_b',
                    master = false
                },
            },
        },
    },
    bucket_count = 4096,
}

log.info('Call %s', box.func['routerc.router_cfg']:call({config}))
log.info('Call %s', box.func['routerc.router_callrw']:call({0, "read", 'bench_call_echo', {1,1,1}}))

--conn:call('routerc.router_callrw', {0, "read", func, args})

