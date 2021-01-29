--
-- 'Ref' module helps to ensure that all buckets on the storage stay writable
-- while there is at least one ref on the storage.
-- Having storage referenced allows to execute any kinds of requests on all the
-- visible data in all spaces in locally stored buckets. This is useful when
-- need to access tons of buckets at once, especially when exact bucket IDs are
-- not known.
--
-- Refs have deadlines. So as the storage wouldn't freeze not being able to move
-- buckets until restart in case a ref is not deleted due to an error in user's
-- code or disconnect.
--
-- The disconnects and restarts mean the refs can't be global. Otherwise any
-- kinds of global counters, uuids and so on, even paired with any ids from a
-- client could clash between clients on their reconnects or storage restarts.
-- Unless they establish a TCP-like session, which would be too complicated.
--
-- Instead, the refs are spread over the existing box sessions. This allows to
-- bind refs of each client to its TCP connection and not care about how to make
-- them unique across all sessions, how not to mess the refs on restart, and how
-- to drop the refs when a client disconnects.
--

local MODULE_INTERNALS = '__module_vshard_storage_ref'
-- Update when change behaviour of anything in the file, to be able to reload.
local MODULE_VERSION = 1

local lfiber = require('fiber')
local lheap = require('vshard.heap')
local lerror = require('vshard.error')
local lconsts = require('vshard.consts')
local lregistry = require('vshard.registry')
local fiber_clock = lfiber.clock
local fiber_yield = lfiber.yield
local DEADLINE_INFINITY = lconsts.DEADLINE_INFINITY
local LUA_CHUNK_SIZE = lconsts.LUA_CHUNK_SIZE

--
-- Binary heap sort. Object with the closest deadline should be on top.
--
local function heap_min_deadline_cmp(ref1, ref2)
    return ref1.deadline < ref2.deadline
end

local M = rawget(_G, MODULE_INTERNALS)
if not M then
    M = {
        module_version = MODULE_VERSION,
        -- Total number of references in all sessions.
        count = 0,
        -- Heap of session objects. Each session has refs sorted by their
        -- deadline. The sessions themselves are also sorted by deadlines.
        -- Session deadline is defined as the closest deadline of all its refs.
        -- Or infinity in case there are no refs in it.
        session_heap = lheap.new(heap_min_deadline_cmp),
        -- Map of session objects. This is used to get session object by its ID.
        session_map = {},
        -- On session disconnect trigger to kill the dead sessions. It is saved
        -- here for the sake of future reload to be able to delete the old
        -- on disconnect function before setting a new one.
        on_disconnect = nil,
    }
else
    -- No reload so far. This is a first version. Return as is.
    return M
end

local function ref_session_new(sid)
    -- Session object does not store its internal hot attributes in a table.
    -- Because it would mean access to any session attribute would cost at least
    -- one table indexing operation. Instead, all internal fields are stored as
    -- upvalues referenced by the methods defined as closures.
    --
    -- This means session creation may not very suitable for jitting, but it is
    -- very rare and attempts to optimize the most common case.
    --
    -- Still the public functions take 'self' object to make it look normally.
    -- They even use it a bit.

    -- Ref map to get ref object by its ID.
    local ref_map = {}
    -- Ref heap sorted by their deadlines.
    local ref_heap = lheap.new(heap_min_deadline_cmp)
    -- Total number of refs of the session. Is used to drop the session when it
    -- it is disconnected. Heap size can't be used because not all refs are
    -- stored here.
    local ref_count_total = 0
    -- Number of refs in use. They are included into the total count. The used
    -- refs are accounted explicitly in order to detect when a disconnected
    -- session has no used refs anymore and can be deleted.
    local ref_count_use = 0
    -- When the session becomes disconnected, it must be deleted from the global
    -- heap when all its used refs are gone.
    local is_disconnected = false
    -- Cache global session storages as upvalues to save on M indexing.
    local global_heap = M.session_heap
    local global_map = M.session_map
    local sched = lregistry.storage_sched

    local function ref_session_discount(self, del_count)
        local new_count = M.count - del_count
        assert(new_count >= 0)
        M.count = new_count

        new_count = ref_count_total - del_count
        assert(new_count >= 0)
        ref_count_total = new_count

        sched.ref_end(del_count)
    end

    local function ref_session_delete_if_not_used(self)
        if not is_disconnected or ref_count_use > 0 then
            return
        end
        ref_session_discount(self, ref_count_total)
        global_map[sid] = nil
        global_heap:remove(self)
    end

    local function ref_session_update_deadline(self)
        local ref = ref_heap:top()
        if not ref then
            self.deadline = DEADLINE_INFINITY
            global_heap:update(self)
        else
            local deadline = ref.deadline
            if deadline ~= self.deadline then
                self.deadline = deadline
                global_heap:update(self)
            end
        end
    end

    --
    -- Garbage collect at most 2 expired refs. The idea is that there is no a
    -- dedicated fiber for expired refs collection. It would be too expensive to
    -- wakeup a fiber on each added or removed or updated ref.
    --
    -- Instead, ref GC is mostly incremental and works by the principle "remove
    -- more than add". On each new ref added, two old refs try to expire. This
    -- way refs don't stack infinitely, and the expired refs are eventually
    -- removed. Because removal is faster than addition: -2 for each +1.
    --
    local function ref_session_gc_step(self, now)
        -- This is inlined 2 iterations of the more general GC procedure. The
        -- latter is not called in order to save on not having a loop,
        -- additional branches and variables.
        if self.deadline > now then
            return
        end
        local top = ref_heap:pop()
        ref_map[top.id] = nil
        top = ref_heap:top()
        if not top then
            self.deadline = DEADLINE_INFINITY
            global_heap:update(self)
            ref_session_discount(self, 1)
            return
        end
        local deadline = top.deadline
        if deadline >= now then
            self.deadline = deadline
            global_heap:update(self)
            ref_session_discount(self, 1)
            return
        end
        ref_heap:remove_top()
        ref_map[top.id] = nil
        top = ref_heap:top()
        if not top then
            self.deadline = DEADLINE_INFINITY
        else
            self.deadline = top.deadline
        end
        global_heap:update(self)
        ref_session_discount(self, 2)
    end

    --
    -- GC expired refs until they end or the limit on the number of iterations
    -- is exhausted. The limit is supposed to prevent too long GC which would
    -- occupy TX thread unfairly.
    --
    -- Returns nil if nothing to GC, or number of iterations left from the
    -- limit. The caller is supposed to yield when 0 is returned, and retry GC
    -- until it returns nil.
    -- The function itself does not yield, because it is used from a more
    -- generic function GCing all sessions. It would not ever yield if all
    -- sessions would have less than limit refs, even if total ref count would
    -- be much bigger.
    --
    -- Besides, the session might be killed during general GC. There must not be
    -- any yields in session methods so as not to introduce a support of dead
    -- sessions.
    --
    local function ref_session_gc(self, limit, now)
        if self.deadline >= now then
            return nil
        end
        local top = ref_heap:top()
        local del = 1
        local rest = 0
        local deadline
        repeat
            ref_heap:remove_top()
            ref_map[top.id] = nil
            top = ref_heap:top()
            if not top then
                self.deadline = DEADLINE_INFINITY
                rest = limit - del
                break
            end
            deadline = top.deadline
            if deadline >= now then
                self.deadline = deadline
                rest = limit - del
                break
            end
            del = del + 1
        until del >= limit
        ref_session_discount(self, del)
        global_heap:update(self)
        return rest
    end

    local function ref_session_add(self, rid, deadline, now)
        if ref_map[rid] then
            return nil, lerror.vshard(lerror.code.STORAGE_REF_ADD,
                                      'duplicate ref')
        end
        local ref = {
            deadline = deadline,
            id = rid,
            -- Used by the heap.
            index = -1,
        }
        ref_session_gc_step(self, now)
        ref_map[rid] = ref
        ref_heap:push(ref)
        if deadline < self.deadline then
            self.deadline = deadline
            global_heap:update(self)
        end
        ref_count_total = ref_count_total + 1
        M.count = M.count + 1
        return true
    end

    --
    -- Ref use means it can't be expired until deleted explicitly. Should be
    -- done when the request affecting the whole storage starts. After use it is
    -- important to call del afterwards - GC won't delete it automatically now.
    -- Unless the entire session is killed.
    --
    local function ref_session_use(self, rid)
        local ref = ref_map[rid]
        if not ref then
            return nil, lerror.vshard(lerror.code.STORAGE_REF_USE, 'no ref')
        end
        ref_heap:remove(ref)
        ref_session_update_deadline(self)
        ref_count_use = ref_count_use + 1
        return true
    end

    local function ref_session_del(self, rid)
        local ref = ref_map[rid]
        if not ref then
            return nil, lerror.vshard(lerror.code.STORAGE_REF_DEL, 'no ref')
        end
        ref_map[rid] = nil
        if ref.index == -1 then
            ref_session_update_deadline(self)
            ref_session_discount(self, 1)
            ref_count_use = ref_count_use - 1
            ref_session_delete_if_not_used(self)
        else
            ref_heap:remove(ref)
            ref_session_update_deadline(self)
            ref_session_discount(self, 1)
        end
        return true
    end

    local function ref_session_kill(self)
        assert(not is_disconnected)
        is_disconnected = true
        ref_session_delete_if_not_used(self)
    end

    -- Don't use __index. It is useless since all sessions use closures as
    -- methods. Also it is probably slower because on each method call would
    -- need to get the metatable, get __index, find the method here. While now
    -- it is only an index operation on the session object.
    local session = {
        deadline = DEADLINE_INFINITY,
        -- Used by the heap.
        index = -1,
        -- Methods.
        del = ref_session_del,
        gc = ref_session_gc,
        add = ref_session_add,
        use = ref_session_use,
        kill = ref_session_kill,
    }
    global_map[sid] = session
    global_heap:push(session)
    return session
end

local function ref_gc()
    local session_heap = M.session_heap
    local session = session_heap:top()
    if not session then
        return
    end
    local limit = LUA_CHUNK_SIZE
    local now = fiber_clock()
    repeat
        limit = session:gc(limit, now)
        if not limit then
            return
        end
        if limit == 0 then
            fiber_yield()
            limit = LUA_CHUNK_SIZE
            now = fiber_clock()
        end
        session = session_heap:top()
    until not session
end

local function ref_add(rid, sid, timeout)
    local now = fiber_clock()
    local deadline = now + timeout
    local ok, err, session
    local storage = lregistry.storage
    local sched = lregistry.storage_sched

    timeout, err = sched.ref_start(timeout)
    if not timeout then
        return nil, err
    end

    while not storage.bucket_are_all_rw() do
        ok, err = storage.bucket_generation_wait(timeout)
        if not ok then
            goto fail_sched
        end
        now = fiber_clock()
        timeout = deadline - now
    end
    session = M.session_map[sid]
    if not session then
        session = ref_session_new(sid)
    end
    ok, err = session:add(rid, deadline, now)
    if ok then
        return true
    end
::fail_sched::
    sched.ref_end(1)
    return nil, err
end

local function ref_use(rid, sid)
    local session = M.session_map[sid]
    if not session then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_USE, 'no session')
    end
    return session:use(rid)
end

local function ref_del(rid, sid)
    local session = M.session_map[sid]
    if not session then
        return nil, lerror.vshard(lerror.code.STORAGE_REF_DEL, 'no session')
    end
    return session:del(rid)
end

local function ref_next_deadline()
    local session = M.session_heap:top()
    return session and session.deadline or DEADLINE_INFINITY
end

local function ref_kill_session(sid)
    local session = M.session_map[sid]
    if session then
        session:kill()
    end
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

M.del = ref_del
M.gc = ref_gc
M.add = ref_add
M.use = ref_use
M.cfg = ref_cfg
M.kill = ref_kill_session
M.next_deadline = ref_next_deadline
lregistry.storage_ref = M

return M
