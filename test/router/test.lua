#!/usr/bin/env tarantool

require('strict').on()

box.cfg{
    listen              = os.getenv("LISTEN"),
}

function failover_wakeup(router)
    local router = router or vshard.router.internal.static_router
    local replicasets = router.replicasets
    for _, rs in pairs(replicasets) do
        rs.worker:wakeup_service('failover')
        for _, r in pairs(rs.replicas) do
            r.worker:wakeup_service('failover')
        end
    end
end

require('console').listen(os.getenv('ADMIN'))
