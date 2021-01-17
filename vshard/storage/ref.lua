local MODULE_INTERNALS = '__module_vshard_storage_ref'
local MODULE_VERSION = 1

local lfiber = require('fiber')
local lheap = require('vshard.heap')
local lerror = require('vshard.error')
local lconsts = require('vshard.consts')
local lregistry = require('vshard.registry')
local clock = lfiber.clock
local yield = lfiber.yield

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        module_version = MODULE_VERSION,
        heap_meta = {},

        count = 0,
        session_heap = lheap.new_min(),
        session_map = {},
        on_disconnect = nil,
    }
else
    return M
end

local heap_meta = M.heap_meta

local function ref_lt(ref1, ref2)
    return ref1.deadline < ref2.deadline
end

local function ref_le(ref1, ref2)
    return ref1.deadline <= ref2.deadline
end

local function ref_eq(ref1, ref2)
    return ref1.deadline == ref2.deadline
end

local function ref_session_del(session, count)
    local new_count = M.count - count
    assert(new_count >= 0)
    M.count = new_count

    new_count = session.count - count
    assert(new_count >= 0)
    session.count = new_count
end

local function ref_session_gc_two(session, now)
    local heap = session.ref_heap
    local top = heap:top()
    if not top then
        return
    end
    if top.deadline > now then
        return
    end
    local map = session.ref_map
    heap:remove_top()
    map[top.id] = nil
    top = heap:top()
    if not top then
        ref_session_del(session, 1)
        return
    end
    if top.deadline > now then
        return
    end
    heap:remove_top()
    map[top.id] = nil
    ref_session_del(session, 2)
end

local function ref_session_update_deadline(session)
    local heap = session.ref_heap
    local ref = heap:top()
    if not ref then
        session.deadline = lconsts.TIMEOUT_INFINITY
        M.session_heap:update(session)
    else
        local deadline = ref.deadline
        if deadline ~= session.deadline then
            session.deadline = deadline
            M.session_heap:update(session)
        end
    end
end

local function ref_gc()
    local session_heap = M.session_heap
    local session = session_heap:top()
    if not session then
        return
    end
    local now = clock()
    if session.deadline >= now then
        return
    end
    local limit = lconsts.LUA_CHUNK_SIZE
    local loop = 0
    repeat
        local heap = session.ref_heap
        local map = session.ref_map
        local top = heap:top()
        local todel = 0
        while loop < limit do
            loop = loop + 1
            todel = todel + 1
            heap:remove_top()
            map[top.id] = nil
            top = heap:top()
            if not top then
                session.deadline = lconsts.TIMEOUT_INFINITY
                break
            end
            if top.deadline >= now then
                session.deadline = top.deadline
                break
            end
        end
        ref_session_del(session, todel)
        session_heap:update(session)
        if loop == limit then
            yield()
            loop = 0
            now = clock()
        end

        session = session_heap:top()
        if not session then
            return
        end
    until session.deadline >= now
end

local function ref_add(rid, sid, timeout)
    local now = clock()
    local deadline = now + timeout
    local session_heap, session_map, session, map, heap, ref, ok, err
    local storage = lregistry.storage
    while M.count == 0 and not storage.bucket_is_all_rw() do
        ok, err = storage.bucket_generation_wait(timeout)
        if not ok then
            return nil, err
        end
        now = clock()
        timeout = deadline - now
        if timeout < 0 then
            return nil, lerror.make(box.error.new(box.error.TIMEOUT))
        end
    end

    session_heap = M.session_heap
    session_map = M.session_map
    session = session_map[sid]
    if not session then
        session = setmetatable({
            deadline = deadline,
            count = 0,
            ref_map = {},
            ref_heap = lheap.new_min(),
        }, heap_meta)
        session_map[sid] = session
        session_heap:push(session)
    end
    map = session.ref_map
    if map[rid] then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_ADD, 'duplicate ref')
    end
    heap = session.ref_heap
    ref = setmetatable({
        deadline = deadline,
        id = rid,
    }, heap_meta)

    ref_session_gc_two(session, now)
    map[rid] = ref
    heap:push(ref)
    if deadline < session.deadline then
        session.deadline = deadline
        session_heap:update(session)
    end
    session.count = session.count + 1
    M.count = M.count + 1
    return true
end

local function ref_use(rid, sid)
    local session = M.session_map[sid]
    if not session then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_USE, 'no session')
    end
    local map = session.ref_map
    local ref = map[rid]
    if not ref then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_USE, 'no ref')
    end
    local heap = session.ref_heap
    heap:remove_try(ref)
    ref_session_update_deadline(session)
    return true
end

local function ref_del(rid, sid)
    local session = M.session_map[sid]
    if not session then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_DEL, 'no session')
    end
    local map = session.ref_map
    local ref = map[rid]
    if not ref then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_DEL, 'no ref')
    end
    local heap = session.ref_heap
    heap:remove_try(ref)
    map[rid] = nil
    ref_session_del(session, 1)
    ref_session_update_deadline(session)
    return true
end

local function ref_kill_session(sid)
    local session = M.session_map[sid]
    if not session then
        return
    end
    M.session_map[sid] = nil
    M.session_heap:remove(session)
    ref_session_del(session, session.count)
end

local function ref_on_session_disconnect()
    ref_kill_session(box.session.id())
end

local function ref_cfg()
    if M.on_disconnect then
        pcall(box.session.on_disconnect, nil, M.on_disconnect)
    end
    box.session.on_disconnect(ref_on_session_disconnect)
    M.on_disconnect = ref_on_session_disconnect
end

heap_meta.__lt = ref_lt
heap_meta.__le = ref_le
heap_meta.__eq = ref_eq

M.del = ref_del
M.gc = ref_gc
M.add = ref_add
M.use = ref_use
M.cfg = ref_cfg
lregistry.storage_ref = M

return M
