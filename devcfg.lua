local USERNAME = 'storage'
local PASSWORD = 'storage'

local function mkuri(port)
    return string.format("%s:%s@%s:%s", USERNAME, PASSWORD, '127.0.0.1', port)
end

return {
    memtx_memory = 100 * 1024 * 1024,
    sharding = {

        { -- replicaset #1
            { uri = mkuri(3301), name = 'storage_1_a', master = true },
            { uri = mkuri(3302), name = 'storage_1_b' },
        },

        { -- replicaset #2
            { uri = mkuri(3303), name = 'storage_2_a', master = true },
            { uri = mkuri(3304), name = 'storage_2_b' },
        },

    }
}
