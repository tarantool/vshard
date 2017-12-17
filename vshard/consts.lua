return {
    PROTO = {
        OK = 0x00,
        BOX_ERROR = 0x01,
        WRONG_BUCKET = 0x02,
        NON_MASTER = 0x03,
        BUCKET_ALREADY_EXISTS = 0x04,
        NO_SUCH_REPLICASET = 0x05,
        MOVE_TO_SELF = 0x06
    },

    -- Bucket FSM
    BUCKET = {
        ACTIVE = 'active',
        SENDING = 'sending',
        SENT = 'sent',
        RECEIVING = 'receiving',
    },

    BUCKET_COUNT = 3000;
    BUCKET_SYNC_TIMEOUT = 0.1;
    CALL_TIMEOUT = 0.1;
    RECONNECT_TIMEOUT = 0.5;
    SYNC_TIMEOUT = 1;
}
