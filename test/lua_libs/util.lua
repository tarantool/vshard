local function check_error(func, ...)
    local pstatus, status, err = pcall(func, ...)
    if pstatus then
        return status, err
    end
    err = string.gsub(status, '.*/[a-z]+.lua.*[0-9]+: ', '')
    return err
end

local function shuffle_masters(cfg)
    for replicaset_uuid, replicaset in pairs(cfg.sharding) do
        local old_master = nil
        local new_master = nil
        for instance_uuid, replica in pairs(replicaset.replicas) do
            if replica.master then
                old_master = replica
            else
                new_master = replica
            end
        end
        old_master.master = nil
        new_master.master = true
    end
end

function collect_timeouts(rs)
    local timeouts = {}
    for uuid, replica in pairs(rs.replicas) do
        table.insert(timeouts, {ok = replica.net_sequential_ok,
                    fail = replica.net_sequential_fail,
                    timeout = replica.net_timeout})
    end
    return timeouts
end

return {
    check_error = check_error,
    shuffle_masters = shuffle_masters,
    collect_timeouts = collect_timeouts,
}
