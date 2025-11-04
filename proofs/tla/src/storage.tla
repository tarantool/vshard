-------------------------------- MODULE storage --------------------------------

(*
 * This module specifies VShard's storage module. It focuses on user requests,
 * which refs and unrefs buckets, and rebalancing services. The only
 * reconfiguration, which can happen - master change, there's no limit, how
 * many masters may be in one replicaset, since failover and configuration
 * update never cannot guarantee one leader in the cluster at one moment (even
 * raft).
 *)

EXTENDS Sequences, Integers, FiniteSets, TLC

--------------------------------------------------------------------------------
\* Declaration.
--------------------------------------------------------------------------------

CONSTANTS
    Storages,          \* Set of all storage IDs.
    ReplicaSets,       \* Set of all replicaset IDs.
    BucketIds,         \* Set of all bucket IDs.
    StorageAssignments,\* [rs |-> SUBSET Storages] - servers to replicaset.
    BucketAssignments, \* [rs |-> SUBSET BucketIds] - buckets to replicaset.
    MasterAssignments  \* [rs |-> SUBSET Storages] - masters to replicaset.

(******************************************************************************)
(* Constants.                                                                 *)
(******************************************************************************)

NULL == "NULL"

\* Pin logic is too trivial, it's not needed in TLA.
BucketState == {"ACTIVE", "SENDING", "SENT", "RECEIVING", "GARBAGE", NULL}
WritableStates == {"ACTIVE"}
ReadableStates == WritableStates \union {"SENDING"}
TransferStates == {"SENDING", "RECEIVING"}

(******************************************************************************)
(* Protocol description.                                                      *)
(******************************************************************************)

\* Model of the network: network[sender][receiver].
VARIABLE network

MessageType == {
    \* bucket_send().
    "BUCKET_RECV",
    "BUCKET_RECV_RESPONSE",
    \* recovery_step_by_step().
    "RECOVERY_BUCKET_STAT",
    "RECOVERY_BUCKET_STAT_RESPONSE",
    \* gc_bucket_process_sent_one_batch_xc().
    "BUCKET_TEST_GC",
    "BUCKET_TEST_GC_RESPONSE",
    \* Replication of the _bucket space.
    "REPLICATION_BUCKET"
}

BucketRecvContent ==
    [bucket: BucketIds, final: BOOLEAN, generation : Nat]
BucketRecvResponseContent ==
    [bucket: BucketIds, status: BOOLEAN]
RecoveryBucketStatContent ==
    [bucket : BucketIds]
RecoveryBucketStatResponseContent ==
    [bucket: BucketIds, status: BucketState, transferring: BOOLEAN]
BucketTestGcContent ==
    [bucket : BucketIds]
BucketTestGcResponseContent ==
    [bucket: BucketIds, can_gc: BOOLEAN]
ReplicationBucketContent ==
    [bucket: BucketIds, status: BucketState,
     destination: ReplicaSets \union {NULL}, generation: Nat]

MessageContent ==
    BucketRecvContent \union
    BucketRecvResponseContent \union
    RecoveryBucketStatContent \union
    RecoveryBucketStatResponseContent \union
    BucketTestGcContent \union
    BucketTestGcResponseContent \union
    ReplicationBucketContent

Message ==
    [type : MessageType, content : MessageContent]

(******************************************************************************)
(* Storage description.                                                       *)
(******************************************************************************)

\* Storage model.
VARIABLES storages, storageToReplicaset

StorageType == [
    vclock : [Storages -> Nat],
    status : {"master", "replica"},
    transferingBuckets : SUBSET BucketIds,
    buckets :
        [BucketIds ->
            [status : BucketState,
             destination : ReplicaSets \union {NULL},
             generation : Nat]],
    bucketRefs : [BucketIds ->
        [ro : Nat, rw : Nat, ro_lock : BOOLEAN, rw_lock : BOOLEAN]],
    gcAck : [BucketIds -> SUBSET Storages],
    sendWaitTarget : [BucketIds -> [Storages -> Nat]],
    sendingBuckets : SUBSET BucketIds,
    masterWaitTarget : [Storages -> Nat],
    \* For limiting tests, it cannot be done outside of the module, since
    \* a test doesn't know, whether the bucket send or ref actually happened.
    errinj : [
        bucketSendCount : Nat,
        bucketRWRefCount : Nat,
        bucketRORefCount : Nat,
        bucketRWUnRefCount : Nat,
        bucketROUnRefCount : Nat,
        networkReorderCount : Nat,
        networkDropCount : Nat,
        masterTransitionCount : Nat,
        replicaTransitionCount : Nat
    ]
]

--------------------------------------------------------------------------------
\* Invariants.
--------------------------------------------------------------------------------

NetworkTypeInv == network \in [Storages -> [Storages -> Seq(Message)]]
StorageTypeInv == storages \in [Storages -> StorageType]
StorageToReplicasetTypeInv == storageToReplicaset \in [Storages -> ReplicaSets]

--------------------------------------------------------------------------------
\* Helpers.
--------------------------------------------------------------------------------

\* Sets root.variable = value for server i.
VarSet(i, variable, value, root) ==
    [root EXCEPT ![i] = [root[i] EXCEPT ![variable] = value]]

IsMaster(state) == state.status = "master"
ReplicasetOf(id) == storageToReplicaset[id]
OtherReplicasInReplicaset(id) == {s \in Storages :
    storageToReplicaset[s] = storageToReplicaset[id]} \ {id}

--------------------------------------------------------------------------------
\* Implementation.
--------------------------------------------------------------------------------

StorageInit ==
    storages = [i \in Storages |-> [
        vclock |-> [j \in Storages |-> 0],
        status |-> IF \E rs \in ReplicaSets : i \in MasterAssignments[rs]
                   THEN "master" ELSE "replica",
        transferingBuckets |-> {},
        buckets |-> [b \in BucketIds |->
            LET rs_for_s ==
               CHOOSE rs \in ReplicaSets : i \in StorageAssignments[rs]
            IN IF b \in BucketAssignments[rs_for_s]
               THEN [status |-> "ACTIVE", destination |-> NULL, generation |-> 1]
               ELSE [status |-> NULL, destination |-> NULL, generation |-> 0]],
        bucketRefs |-> [b \in BucketIds |->
            [ro |-> 0, rw |-> 0, ro_lock |-> FALSE, rw_lock |-> FALSE]],
        gcAck |-> [b \in BucketIds |-> {}],
        sendWaitTarget |-> [b \in BucketIds |-> [s \in Storages |-> 0]],
        sendingBuckets |-> {},
        masterWaitTarget |-> [s \in Storages |-> 0],
        errinj |-> [
            bucketSendCount |-> 0,
            bucketRWRefCount |-> 0,
            bucketRORefCount |-> 0,
            bucketRWUnRefCount |-> 0,
            bucketROUnRefCount |-> 0,
            networkReorderCount |-> 0,
            networkDropCount |-> 0,
            masterTransitionCount |-> 0,
            replicaTransitionCount |-> 0
        ]
    ]]

Init ==
    /\ StorageInit
    /\ storageToReplicaset = [s \in Storages |->
            CHOOSE rs \in ReplicaSets : s \in StorageAssignments[rs]]
    /\ network = [i \in Storages |-> [j \in Storages |-> <<>>]]

StorageState(i) == [
    networkSend |-> network[i],
    networkReceive |-> [n \in Storages |-> network[n][i]],

    id |-> i,
    vclock |-> storages[i].vclock,
    status |-> storages[i].status,
    transferingBuckets |-> storages[i].transferingBuckets,
    buckets |-> storages[i].buckets,
    bucketRefs |-> storages[i].bucketRefs,
    gcAck |-> storages[i].gcAck,
    sendWaitTarget |-> storages[i].sendWaitTarget,
    sendingBuckets |-> storages[i].sendingBuckets,
    masterWaitTarget |-> storages[i].masterWaitTarget,
    errinj |-> storages[i].errinj
]

StorageStateApply(i, state) ==
    /\ storages' = VarSet(i, "vclock", state.vclock,
                   VarSet(i, "status", state.status,
                   VarSet(i, "transferingBuckets", state.transferingBuckets,
                   VarSet(i, "buckets", state.buckets,
                   VarSet(i, "bucketRefs", state.bucketRefs,
                   VarSet(i, "gcAck", state.gcAck,
                   VarSet(i, "sendWaitTarget", state.sendWaitTarget,
                   VarSet(i, "sendingBuckets", state.sendingBuckets,
                   VarSet(i, "masterWaitTarget", state.masterWaitTarget,
                   VarSet(i, "errinj", state.errinj, storages))))))))))
    /\ network' =
        [s \in Storages |->
            [t \in Storages |->
                IF s = i THEN
                    \* Messages sent from i
                    state.networkSend[t]
                ELSE IF t = i THEN
                    \* Messages received by i
                    state.networkReceive[s]
                ELSE
                    \* Unchanged
                    network[s][t]
            ]
        ]
    /\ UNCHANGED <<storageToReplicaset>>

(***************************************************************************)
(*                              Replication                                *)
(***************************************************************************)

BucketStatusChange(state, from, bucket, status, destination, generation) ==
    LET ref_before == state.bucketRefs[bucket]
        ref_after == [ref_before EXCEPT
                !.ro_lock = ~(status \in ReadableStates),
                !.rw_lock = ~(status \in WritableStates)]
        state1 == [state EXCEPT
        !.buckets[bucket] = [status |-> status, destination |-> destination,
                             generation |-> generation],
        !.bucketRefs[bucket] = ref_after,
        !.vclock[from] = @ + 1]
        a == Assert(state.status = "master", "Bucket change on non-master") IN
    IF from = state.id /\ OtherReplicasInReplicaset(state.id) # {} THEN
        \* If this node is the source of the change,
        \* replicate to all other nodes in replicaset
        LET replication_msg == [type |-> "REPLICATION_BUCKET",
            content |-> [bucket |-> bucket, status |-> status,
            destination |-> destination, generation |-> generation]] IN
        [state1 EXCEPT !.networkSend = [
            j \in Storages |-> IF j \in OtherReplicasInReplicaset(state.id)
                               THEN Append(state1.networkSend[j], replication_msg)
                               ELSE state1.networkSend[j]]]
    ELSE
        state1

ProcessReplicationBucket(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type = "REPLICATION_BUCKET" THEN
        LET stateNew == BucketStatusChange(state, j, msg.content.bucket,
            msg.content.status, msg.content.destination, msg.content.generation)
        IN [stateNew EXCEPT !.networkReceive[j] = Tail(@)]
    ELSE state

(***************************************************************************)
(*                        Failover master change                           *)
(***************************************************************************)

\* True iff this node's current vclock has reached the stored target.
MasterSyncDone(state) ==
    \A s \in Storages : state.vclock[s] >= state.masterWaitTarget[s]

\* Max of a non-empty set of Nats.
MaxNat(S) == CHOOSE m \in S : \A x \in S : m >= x

\* Compute the one-shot target when i becomes master:
\* for every source s, the maximum vclock[s] seen among peers at that instant.
MasterCatchupTarget(i) ==
    [ s \in Storages |->
        IF OtherReplicasInReplicaset(i) = {} THEN 0
        ELSE MaxNat({storages[p].vclock[s] : p \in OtherReplicasInReplicaset(i)})
    ]

\* Until master synchronizes with other repliacas, rebalancer and recovery
\* are not allowed.
BecomeMaster(state) ==
    IF ~IsMaster(state) THEN
        LET target == MasterCatchupTarget(state.id) IN [state EXCEPT
        !.status = "master", !.errinj.masterTransitionCount = @ + 1,
        !.masterWaitTarget = target]
    ELSE state

BecomeReplica(state) ==
    IF IsMaster(state) THEN [state EXCEPT
        !.status = "replica", !.errinj.replicaTransitionCount = @ + 1]
    ELSE state

(***************************************************************************)
(*                             Storage call                                *)
(***************************************************************************)

BucketRef(state, bucket, mode) ==
    LET ref == state.bucketRefs[bucket]
        bucketState == state.buckets[bucket].status IN
    IF mode = "read" THEN
        \* Read request allowed if bucket is readable and not locked.
        IF ~(bucketState \in ReadableStates) \/ ref.ro_lock THEN state
        ELSE [state EXCEPT
                !.bucketRefs[bucket].ro = @ + 1,
                !.errinj.bucketRORefCount = @ + 1]
    ELSE
        \* Write request allowed only if bucket writable and storage is master.
        IF ~(bucketState \in WritableStates) \/ ref.rw_lock \/ ~IsMaster(state)
        THEN state ELSE [state EXCEPT
                !.bucketRefs[bucket].rw = @ + 1,
                !.errinj.bucketRWRefCount = @ + 1]

BucketUnRef(state, bucket, mode) ==
    LET ref == state.bucketRefs[bucket] IN
    IF mode = "read" THEN
        IF ref.ro = 0 THEN state
        ELSE [state EXCEPT
                !.bucketRefs[bucket].ro = @ - 1,
                !.errinj.bucketROUnRefCount = @ + 1]
    ELSE
        IF ref.rw = 0 THEN state
        ELSE [state EXCEPT
                !.bucketRefs[bucket].rw = @ - 1,
                !.errinj.bucketRWUnRefCount = @ + 1]

(***************************************************************************)
(*                           Bucket sending                                *)
(***************************************************************************)

AllReplicasCaughtUp(state, b) ==
    LET rs == ReplicasetOf(state.id)
        replicas == OtherReplicasInReplicaset(state.id)
        target == state.sendWaitTarget[b]
        m == state.id
    IN
        \A r \in replicas : storages[r].vclock[m] >= target[m]

BucketSendWaitAndSend(state, b, j) ==
    IF IsMaster(state)
       /\ state.buckets[b].status = "SENDING"
       /\ ReplicasetOf(j) = state.buckets[b].destination
       /\ b \in state.transferingBuckets
       /\ b \in state.sendingBuckets
       /\ j # state.id
       /\ AllReplicasCaughtUp(state, b)
       /\ MasterSyncDone(state)
    THEN
        LET msg == [type |-> "BUCKET_RECV",
                    content |-> [bucket |-> b, final |-> FALSE,
                    generation |-> state.buckets[b].generation]]
        IN [state EXCEPT
                !.networkSend[j] = Append(@, msg),
                !.sendingBuckets = @ \ {b}]
    ELSE
        state

BucketDropFromSending(state, b) ==
    IF b \in state.sendingBuckets THEN
        [state EXCEPT !.sendingBuckets = @ \ {b}]
    ELSE
        state

BucketSendStart(state, b, j) ==
    IF IsMaster(state) /\ state.buckets[b].status = "ACTIVE" /\
       state.bucketRefs[b].rw = 0 /\ ~state.bucketRefs[b].rw_lock /\
       b \notin state.transferingBuckets /\ j # state.id /\ MasterSyncDone(state)
    THEN LET state1 == [state EXCEPT
                      !.bucketRefs[b].rw_lock = TRUE,
                      !.transferingBuckets = @ \union {b},
                      !.errinj.bucketSendCount = @ + 1] IN
         LET state2 == BucketStatusChange(
                  state1, state.id, b, "SENDING", ReplicasetOf(j),
                  state1.buckets[b].generation + 1)
             state3 == [state2 EXCEPT
                  !.sendWaitTarget[b] = state2.vclock,
                  !.sendingBuckets = @ \union {b}]
         IN BucketSendWaitAndSend(state3, b, j)
    ELSE state

BucketRecvStart(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type = "BUCKET_RECV" /\ ~msg.content.final /\ MasterSyncDone(state) THEN
        LET b == msg.content.bucket IN
        IF IsMaster(state) /\ state.buckets[b].status = NULL /\
           b \notin state.transferingBuckets
        THEN
            \* Valid handoff start - become RECEIVING and ack success.
            LET state1 == [state EXCEPT
                           !.networkReceive[j] = Tail(@)] IN
            LET state2 == BucketStatusChange(
                  state1, state.id, b, "RECEIVING", ReplicasetOf(j),
                  msg.content.generation) IN
            LET response == [type |-> "BUCKET_RECV_RESPONSE",
                             content |-> [bucket |-> b, status |-> TRUE]] IN
            [state2 EXCEPT !.networkSend[j] = Append(@, response)]
        ELSE
            \* Error: reply with failure response instead of silent drop.
            LET response == [type |-> "BUCKET_RECV_RESPONSE",
                             content |-> [bucket |-> b, status |-> FALSE]] IN
            [state EXCEPT
                !.networkReceive[j] = Tail(@),
                !.networkSend[j] = Append(@, response)]
    ELSE state  \* Leave non-BUCKET_RECV messages in queue

BucketSendFinish(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type = "BUCKET_RECV_RESPONSE" THEN
        LET b == msg.content.bucket
            ok == msg.content.status
        IN
        IF IsMaster(state) /\ b \in state.transferingBuckets /\ MasterSyncDone(state) THEN
            LET state1 == [state EXCEPT !.networkReceive[j] = Tail(@)] IN
            IF ok THEN
                \* Receiver accepted the bucket - mark as SENT
                \* and send final message.
                LET state2 == BucketStatusChange(state1, state.id, b, "SENT",
                    ReplicasetOf(j), state1.buckets[b].generation) IN
                LET final_msg ==
                    [ type |-> "BUCKET_RECV",
                      content |-> [bucket |-> b, final |-> TRUE,
                      generation |-> state.buckets[b].generation]] IN
                [ state2 EXCEPT
                    !.transferingBuckets = @ \ {b},
                    !.networkSend[j] = Append(@, final_msg)
                ]
            ELSE
                \* Receiver rejected - stop transfer and clear state.
                [ state1 EXCEPT
                    !.transferingBuckets = @ \ {b} ]
        ELSE
            \* Not master or bucket not being transferred -> drop message.
            [ state EXCEPT !.networkReceive[j] = Tail(@) ]
    ELSE
        \* Leave other message types untouched.
        state

BucketRecvFinish(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type = "BUCKET_RECV" /\ msg.content.final THEN
        LET b == msg.content.bucket IN
        IF IsMaster(state) /\ state.buckets[b].status = "RECEIVING" /\
           b \notin state.transferingBuckets /\ MasterSyncDone(state)
        THEN LET state1 == [state EXCEPT
                            !.networkReceive[j] = Tail(@)] IN
            LET state2 ==
                   BucketStatusChange(state1, state.id, b, "ACTIVE", NULL,
                        state1.buckets[b].generation) IN
               [state2 EXCEPT !.bucketRefs[b].rw_lock = FALSE]
        ELSE [state EXCEPT !.networkReceive[j] = Tail(@)]  \* Drop if not master
    ELSE state  \* Leave non-BUCKET_RECV messages in queue

\* Timeout happens, bucket_send() fails.
BucketDropFromTransfering(state, b) ==
    IF b \in state.transferingBuckets THEN
        [state EXCEPT !.transferingBuckets = @ \ {b}]
    ELSE
        state

(***************************************************************************)
(*                              Recovery                                   *)
(***************************************************************************)

RecoverySendStatRequest(state, b) ==
    LET dest_rs == state.buckets[b].destination IN
    IF IsMaster(state)
       /\ state.buckets[b].status \in {"SENDING", "RECEIVING"}
       /\ ~(b \in state.transferingBuckets)
       /\ dest_rs # NULL
       /\ MasterSyncDone(state)
    THEN
        \* Choose any storage in the destination replicaset.
        LET candidates == {s \in Storages :
                             storageToReplicaset[s] = dest_rs}
            dest == CHOOSE s \in candidates : TRUE
            msg == [type |-> "RECOVERY_BUCKET_STAT",
                    content |-> [bucket |-> b]]
        IN [state EXCEPT !.networkSend[dest] = Append(@, msg)]
    ELSE
        state

ProcessRecoveryStatRequest(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type # "RECOVERY_BUCKET_STAT" THEN state
    ELSE
        IF ~IsMaster(state) \/ ~MasterSyncDone(state) THEN
            \* Drop message if this node is not a master.
            [state EXCEPT !.networkReceive[j] = Tail(@)]
        ELSE
            LET b == msg.content.bucket
                reply == [type |-> "RECOVERY_BUCKET_STAT_RESPONSE",
                          content |-> [
                              bucket |-> b,
                              status |-> state.buckets[b].status,
                              transferring |-> (b \in state.transferingBuckets)
                          ]]
            IN [state EXCEPT
                    !.networkReceive[j] = Tail(@),
                    !.networkSend[j] = Append(@, reply)]

ProcessRecoveryStatResponse(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type # "RECOVERY_BUCKET_STAT_RESPONSE" THEN state
    ELSE
        IF ~IsMaster(state) \/ ~MasterSyncDone(state) THEN
            \* Drop message if this node is not a master.
            [state EXCEPT !.networkReceive[j] = Tail(@)]
        ELSE
            LET b == msg.content.bucket
                remoteStatus == msg.content.status
                remoteTransf == msg.content.transferring
                localStatus == state.buckets[b].status
            IN
            IF ~(localStatus \in TransferStates) THEN
                [state EXCEPT !.networkReceive[j] = Tail(@)]
            \* Recovery policy: sender adjusts state after getting peer's status.
            ELSE IF localStatus = "SENDING" /\ remoteStatus \in {"ACTIVE"} THEN
                LET state1 == [state EXCEPT !.networkReceive[j] = Tail(@)] IN
                BucketStatusChange(state1, state.id, b, "SENT",
                    state.buckets[b].destination, state1.buckets[b].generation)
            ELSE IF localStatus = "RECEIVING" /\
                    (remoteStatus \in WritableStates
                     \/ (remoteStatus = "SENDING" /\ ~remoteTransf)) THEN
                LET state1 == [state EXCEPT !.networkReceive[j] = Tail(@)] IN
                BucketStatusChange(state1, state.id, b, "GARBAGE", NULL,
                    state1.buckets[b].generation)
            ELSE IF (b \notin state.transferingBuckets)
                     /\ (remoteStatus \in {"SENT", "GARBAGE"} \/ remoteStatus = NULL) THEN
                LET state1 == [state EXCEPT !.networkReceive[j] = Tail(@)] IN
                BucketStatusChange(state1, state.id, b, "ACTIVE", NULL,
                    state1.buckets[b].generation)
            ELSE
                [state EXCEPT !.networkReceive[j] = Tail(@)]

(***************************************************************************)
(*                              Garbage Collector                          *)
(***************************************************************************)

\* Master-only background process.
\* It checks SENT buckets that are not transferring and have no refs.
\* It asks all replicas in the same replicaset if they have any RO refs.
\* If all replicas report can_gc = TRUE, it marks the bucket as GARBAGE.
\* Later, GC will delete GARBAGE buckets (make them NULL).

------------------------------------------------------------------------------
\* 1. Send GC check request.
------------------------------------------------------------------------------

TryBucketGarbage(state, b) ==
    LET replicas == OtherReplicasInReplicaset(state.id) IN
    IF state.gcAck[b] = replicas
       /\ state.buckets[b].status = "SENT"
       /\ state.bucketRefs[b].ro = 0
       /\ state.bucketRefs[b].rw = 0
       /\ ~(b \in state.transferingBuckets)
    THEN
        \* Reset acks and mark bucket as GARBAGE
        LET state1 == [state EXCEPT !.gcAck[b] = {}] IN
        BucketStatusChange(state1, state.id, b, "GARBAGE", NULL,
            state1.buckets[b].generation)
    ELSE
        state

GcSendTestRequest(state, b) ==
    IF IsMaster(state)
       /\ state.buckets[b].status = "SENT"
       /\ ~(b \in state.transferingBuckets)
       /\ state.bucketRefs[b].ro = 0
       /\ state.bucketRefs[b].rw = 0
    THEN
        LET rs == ReplicasetOf(state.id)
            dests == OtherReplicasInReplicaset(state.id)
            msg == [type |-> "BUCKET_TEST_GC",
                    content |-> [bucket |-> b]]
        IN
        IF dests = {} THEN
            \* No other replicas - mark immediately if eligible
            TryBucketGarbage(state, b)
        ELSE
            [state EXCEPT
                !.networkSend =
                    [j \in Storages |->
                        IF j \in dests
                        THEN Append(state.networkSend[j], msg)
                        ELSE state.networkSend[j]]]
    ELSE
        state

------------------------------------------------------------------------------
\* 2. Process GC test request.
------------------------------------------------------------------------------

ProcessGcTestRequest(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type # "BUCKET_TEST_GC" THEN state
    ELSE
        IF ~IsMaster(state) THEN
            \* Drop if not master (only masters handle messages)
            [state EXCEPT !.networkReceive[j] = Tail(@)]
        ELSE
            LET b == msg.content.bucket
                \* can_gc is true if this replica has no ro refs and bucket is SENT or GARBAGE
                can_gc == (state.bucketRefs[b].ro = 0)
                           /\ (state.buckets[b].status \in {"SENT", "GARBAGE", NULL})
                reply == [type |-> "BUCKET_TEST_GC_RESPONSE",
                          content |-> [bucket |-> b, can_gc |-> can_gc]]
            IN [state EXCEPT
                    !.networkReceive[j] = Tail(@),
                    !.networkSend[j] = Append(@, reply)]

------------------------------------------------------------------------------
\* 3. Process GC test responses.
------------------------------------------------------------------------------

ProcessGcTestResponse(state, j) ==
    LET msg == Head(state.networkReceive[j]) IN
    IF msg.type # "BUCKET_TEST_GC_RESPONSE" THEN state
    ELSE
        IF ~IsMaster(state) THEN
            [state EXCEPT !.networkReceive[j] = Tail(@)]
        ELSE
            LET b == msg.content.bucket
                can_gc == msg.content.can_gc
                acks_before == state.gcAck[b]
                acks_after ==
                    IF can_gc THEN acks_before \union {j} ELSE acks_before
                state1 == [state EXCEPT
                            !.networkReceive[j] = Tail(@),
                            !.gcAck[b] = acks_after]
            IN TryBucketGarbage(state1, b)

------------------------------------------------------------------------------
\* 4. Delete (NULL) GARBAGE buckets.
------------------------------------------------------------------------------

GcDropGarbage(state, b) ==
    IF IsMaster(state) /\ state.buckets[b].status = "GARBAGE" THEN
        BucketStatusChange(state, state.id, b, NULL, NULL, 0)
    ELSE state

(***************************************************************************)
(*                              Network                                    *)
(***************************************************************************)

\* Is a message a replication one?
IsReplication(m) == m.type = "REPLICATION_BUCKET"

\* Forbid reordering two replication msgs.
CanSwap(m1, m2) == ~(IsReplication(m1) /\ IsReplication(m2))

\* Safe to drop this message?
CanDrop(m) == ~IsReplication(m)

\* Remove element at position k (1-based).
\* If k out-of-range, return the seq unchanged.
SeqRemoveAt(S, k) ==
  IF k < 1 \/ k > Len(S) THEN S
  ELSE
    LET pre  == IF k = 1 THEN << >> ELSE SubSeq(S, 1, k-1)
        post == IF k = Len(S) THEN << >> ELSE SubSeq(S, k+1, Len(S))
    IN pre \o post

SeqSwapAdjacent(s, k) ==
  IF k < 1 \/ k >= Len(s) THEN s
  ELSE
    LET a == s[k]
        b == s[k+1]
    IN [ s EXCEPT ![k] = b, ![k+1] = a ]

\* Reorder: pick any channel and swap one adjacent pair,
\* as long as we are not swapping two REPLICATION_BUCKETs.
ReorderOneNetworkMessage ==
  \E s \in Storages, t \in Storages :
    LET Q == network[s][t] IN
    /\ Len(Q) >= 2
    /\ \E k \in 1..(Len(Q)-1) :
         CanSwap(Q[k], Q[k+1]) /\
         network' = [network EXCEPT ![s][t] = SeqSwapAdjacent(Q, k)] /\
         storages' = [storages EXCEPT ![s].errinj.networkReorderCount = @ + 1]
    /\ UNCHANGED <<storageToReplicaset >>

\* Drop: pick any non-replication message at any position and remove it.
DropOneNetworkMessage ==
  \E s \in Storages, t \in Storages :
    LET Q == network[s][t]
        DroppableIdx == { k \in 1..Len(Q) : CanDrop(Q[k]) }
    IN /\ DroppableIdx # {}
       /\ \E k \in DroppableIdx :
            /\ network' = [network EXCEPT ![s][t] = SeqRemoveAt(Q, k)]
            /\ storages' = [storages EXCEPT ![s].errinj.networkDropCount = @ + 1]
       /\ UNCHANGED <<storageToReplicaset >>

(***************************************************************************)
(*                            Main actions                                 *)
(***************************************************************************)

Next ==
    \/ ReorderOneNetworkMessage
    \/ DropOneNetworkMessage
    \/ \E i \in Storages :
       LET state == StorageState(i)
       IN \/ StorageStateApply(i, BecomeMaster(state))
          \/ StorageStateApply(i, BecomeReplica(state))
          \/ \E j \in Storages :
              /\ Len(state.networkReceive[j]) > 0
              /\ \/ StorageStateApply(i, ProcessReplicationBucket(state, j))
                 \/ StorageStateApply(i, BucketSendFinish(state, j))
                 \/ StorageStateApply(i, BucketRecvStart(state, j))
                 \/ StorageStateApply(i, BucketRecvFinish(state, j))
                 \/ StorageStateApply(i, ProcessRecoveryStatRequest(state, j))
                 \/ StorageStateApply(i, ProcessRecoveryStatResponse(state, j))
                 \/ StorageStateApply(i, ProcessGcTestRequest(state, j))
                 \/ StorageStateApply(i, ProcessGcTestResponse(state, j))
          \/ \E b \in BucketIds, mode \in {"read", "write"} :
               \/ StorageStateApply(i, BucketRef(state, b, mode))
               \/ StorageStateApply(i, BucketUnRef(state, b, mode))
          \/ \E j \in Storages, b \in BucketIds :
               \/ StorageStateApply(i, BucketSendStart(state, b, j))
               \/ StorageStateApply(i, BucketSendWaitAndSend(state, b, j))
          \/ \E b \in BucketIds :
               \/ StorageStateApply(i, RecoverySendStatRequest(state, b))
               \/ StorageStateApply(i, GcDropGarbage(state, b))
               \/ StorageStateApply(i, GcSendTestRequest(state, b))
               \/ StorageStateApply(i, BucketDropFromTransfering(state, b))
               \/ StorageStateApply(i, BucketDropFromSending(state, b))

================================================================================
