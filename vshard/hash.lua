-- hash.lua
local ldigest = require('digest')

--
-- Simple hash function based on crc32
--
local function key_hash(shard_key)
    if type(shard_key) ~= 'table' then
        return ldigest.crc32(tostring(shard_key))
    else
        local crc32 = ldigest.crc32.new()
        for _, v in ipairs(shard_key) do
            crc32:update(tostring(v))
        end
        return crc32:result()
    end
end

return {
    key_hash = key_hash;
}
