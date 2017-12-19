local function check_error(func, ...)
    local status, err = pcall(func, ...)
    assert(not status)
    err = string.gsub(err, '.*/[a-z]+.lua.*[0-9]+: ', '')
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

return {
    check_error = check_error,
    shuffle_masters = shuffle_masters,
}
