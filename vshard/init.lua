local ldigest = require('digest')
local consts = require('vshard.consts')

--
-- Simple hash function based on crc32
--
local function bucket_id(shard_key)
    local crc32 = ldigest.crc32.new()
    for _, v in ipairs(shard_key) do
        crc32:update(tostring(v))
    end
    return crc32:result() % consts.BUCKET_COUNT
end

return {
    bucket_id = bucket_id,
    storage = require('vshard.storage'),
    consts = require('vshard.consts'),
}
