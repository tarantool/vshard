local ldigest = require('digest')

--
-- Simple hash function based on crc32
--
local function bucket_id(shard_key, bucket_count)
    assert(bucket_count ~= nil)
    local crc32 = ldigest.crc32.new()
    for _, v in ipairs(shard_key) do
        crc32:update(tostring(v))
    end
    return crc32:result() % bucket_count
end

return {
    bucket_id = bucket_id,
    router = require('vshard.router'),
    storage = require('vshard.storage'),
    consts = require('vshard.consts'),
    error = require('vshard.error'),
}
