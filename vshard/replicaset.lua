-- vshard.replicaset

--
-- <replicaset> = {
--     replicas = {
--         [replica_uuid] = {
--             uri = string,
--             name = string,
--             uuid = string,
--             conn = <netbox>
--          }
--      },
--      master = <master server from the array above>,
--      uuid = <replicaset_uuid>,
--      weight = number,
--  }
--
-- replicasets = {
--    [replicaset_uuid] = <replicaset>
-- }
--

local log = require('log')
local netbox = require('net.box')
local consts = require('vshard.consts')
local codes = require('vshard.codes')

--
-- on_connect() trigger for net.box
--
local function netbox_on_connect(conn)
    log.info("connected to %s:%s", conn.host, conn.port)
end

--
-- on_disconnect() trigger for net.box
--
local function netbox_on_disconnect(conn)
    log.info("disconnected from %s:%s", conn.host, conn.port)
end

--
-- Create net.box connection to master
--
local function replicaset_connect(replicaset)
    local master = replicaset.master
    if master == nil then
        return nil, {
            code = codes.MISSING_MASTER,
            replicaset_uuid = replicaset.uuid,
        }
    end
    local conn = master.conn
    if conn == nil then
        -- Use wait_connected = false to prevent races on parallel requests
        conn = netbox.connect(master.uri, {
            reconnect_after = consts.RECONNECT_TIMEOUT,
            wait_connected = false
        })
        conn:on_connect(netbox_on_connect)
        conn:on_disconnect(netbox_on_disconnect)
        master.conn = conn
        conn:ping()
    end
    return conn
end

--
-- Destroy net.box connection to master
--
local function replicaset_disconnect(replicaset)
    local master = replicaset.master
    if master == nil then
       return true
    end
    local conn = replicaset.master.conn
    replicaset.master.conn = nil
    conn:close()
    return true
end

--
-- Helper for replicaset_call
--
local function replicaset_call_tail(replicaset, func, pstatus, status, ...)
    if not pstatus then
        log.error("Exception during calling '%s' on '%s': %s", func,
                  replicaset.master.uuid, status)
        return nil, {
            code = codes.BOX_ERROR,
            error = status
        }
    end
    if status == nil then
        status = nil -- Workaround for `not msgpack.NULL` magic.
    end
    return status, ...
end

--
-- Call a function on remote storage
-- Note: this function uses pcall-style error handling
-- @retval false, err on error
-- @retval true, ... on success
--
local function replicaset_call(replicaset, func, args)
    assert(type(func) == 'string', 'function name')
    assert(type(args) == 'table', 'function arguments')
    local conn, err = replicaset_connect(replicaset)
    if conn == nil then
        return nil, err
    end
    return replicaset_call_tail(replicaset, func,
                                pcall(conn.call, conn, func, args,
                                      {timeout = consts.CALL_TIMEOUT}))
end

--
-- Nice formatter for replicaset
--
local function replicaset_tostring(replicaset)
    local uri = ''
    if replicaset.master then
        uri = replicaset.master.uri
    end
    return string.format('Replicaset(uuid=%s, master=%s)',
                         replicaset.uuid, uri)
end

--
-- Meta-methods
--
local replicaset_mt = {
    __index = {
        connect = replicaset_connect;
        disconnect = replicaset_disconnect;
        call = replicaset_call;
    };
    __tostring = replicaset_tostring;
}

--
-- Update/build replicasets from configuration
--
local function buildall(sharding_cfg, existing_replicasets)
    local new_replicasets = {}
    for replicaset_uuid, replicaset in pairs(sharding_cfg) do
        local new_replicaset = setmetatable({
            replicas = {},
            uuid = replicaset_uuid,
            weight = replicaset.weight
        }, replicaset_mt)
        for replica_uuid, replica in pairs(replicaset.replicas) do
            local new_replica = {uri = replica.uri, name = replica.name,
                                 uuid = replica_uuid}
            local existing_rs = existing_replicasets[replicaset_uuid]
            if existing_rs ~= nil and existing_rs.replicas[replica_uuid] then
                new_replica.conn = existing_rs.replicas[replica_uuid].conn
            end
            new_replicaset.replicas[replica_uuid] = new_replica
            if replica.master then
                new_replicaset.master = new_replica
            end
        end
        new_replicasets[replicaset_uuid] = new_replicaset
    end
    return new_replicasets
end

return {
    buildall = buildall;
}
