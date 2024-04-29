return {
    -- Сontains the module version.
    -- Requires manual update in case of release commit.
    VERSION = '0.1.27',

    -- Bucket FSM
    BUCKET = {
        ACTIVE = 'active',
        PINNED = 'pinned',
        SENDING = 'sending',
        SENT = 'sent',
        RECEIVING = 'receiving',
        GARBAGE = 'garbage',
    },

    BUCKET_EVENT = {
        -- Txn triggers allow to attach anything to the bucket transaction. That
        -- was requested by a customer to be able to detect vshard book-keeping
        -- actions like bucket sending right in the journal.
        RECV = 'bucket_data_recv_txn',
        GC = 'bucket_data_gc_txn'
    },

    STATUS = {
        GREEN = 0,
        YELLOW = 1,
        ORANGE = 2,
        RED = 3,
    },

    REPLICATION_THRESHOLD_SOFT = 1,
    REPLICATION_THRESHOLD_HARD = 5,
    REPLICATION_THRESHOLD_FAIL = 10,

    DEFAULT_BUCKET_COUNT = 3000;
    BUCKET_SENT_GARBAGE_DELAY = 0.5;
    BUCKET_CHUNK_SIZE = 1000;
    LUA_CHUNK_SIZE = 100000,
    DEFAULT_REBALANCER_DISBALANCE_THRESHOLD = 1;
    REBALANCER_IDLE_INTERVAL = 60 * 60;
    REBALANCER_WORK_INTERVAL = 10;
    REBALANCER_CHUNK_TIMEOUT = 60 * 5;
    REBALANCER_GET_STATE_TIMEOUT = 5,
    REBALANCER_APPLY_ROUTES_TIMEOUT = 5,
    DEFAULT_REBALANCER_MAX_SENDING = 1;
    REBALANCER_MAX_SENDING_MAX = 15;
    DEFAULT_REBALANCER_MAX_RECEIVING = 100;
    CALL_TIMEOUT_MIN = 0.5;
    CALL_TIMEOUT_MAX = 64;
    FAILOVER_UP_TIMEOUT = 5;
    FAILOVER_DOWN_TIMEOUT = 1;
    DEFAULT_FAILOVER_PING_TIMEOUT = 5;
    DEFAULT_SYNC_TIMEOUT = 1;
    RECONNECT_TIMEOUT = 0.5;
    GC_BACKOFF_INTERVAL = 5,
    GC_MAP_CALL_TIMEOUT = 64,
    GC_WAIT_LSN_TIMEOUT = 64,
    GC_WAIT_LSN_STEP = 0.1,
    RECOVERY_BACKOFF_INTERVAL = 5,
    RECOVERY_GET_STAT_TIMEOUT = 5,
    REPLICA_BACKOFF_INTERVAL = 5,
    REPLICA_NOACTIVITY_TIMEOUT = 60 * 5,
    DEFAULT_BUCKET_SEND_TIMEOUT = 10,
    DEFAULT_BUCKET_RECV_TIMEOUT = 10,

    DEFAULT_SCHED_REF_QUOTA = 300,
    DEFAULT_SCHED_MOVE_QUOTA = 1,

    DISCOVERY_IDLE_INTERVAL = 10,
    DISCOVERY_WORK_INTERVAL = 1,
    DISCOVERY_WORK_STEP = 0.01,
    DISCOVERY_TIMEOUT = 10,

    MASTER_SEARCH_IDLE_INTERVAL = 5,
    MASTER_SEARCH_WORK_INTERVAL = 0.5,
    MASTER_SEARCH_BACKOFF_INTERVAL = 5,
    MASTER_SEARCH_TIMEOUT = 5,

    TIMEOUT_INFINITY = 500 * 365 * 86400,
    DEADLINE_INFINITY = math.huge,
}
