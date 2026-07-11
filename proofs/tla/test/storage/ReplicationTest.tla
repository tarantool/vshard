-------------------------- MODULE ReplicationTest ------------------------------

EXTENDS storage, utils

CONSTANTS b1, b2

StoragesC == {"s1", "s2", "s3"}
ReplicaSetsC == {"rs1"}
BucketIdsC == {b1, b2}
StorageAssignmentsC == [rs1 |-> {"s1", "s2", "s3"}]
BucketAssignmentsC == [rs1 |-> {b1, b2}]
MasterAssignmentsC == [rs1 |-> {"s1", "s2", "s3"}]

(***************************************************************************)
(*                           CONSTRAINTS                                   *)
(***************************************************************************)

MAX_TOTAL_SENDS == 4

\* 1. Limit total bucket sends - prevent endless transfers.
SendLimitConstraint ==
    LET totalSends ==
        SetSum({ storages[i].errinj.bucketSendCount : i \in Storages })
    IN totalSends =< MAX_TOTAL_SENDS

(***************************************************************************)
(*                        Specification                                    *)
(***************************************************************************)

TestBucketStatusChange(state, bucketId) ==
    IF state.status = "master" THEN
        LET newState == CHOOSE newState \in BucketState :
                newState /= state.buckets[bucketId].status
        IN BucketStatusChange(state, state.id, bucketId, newState, NULL)
    ELSE state

TestNext == \E i \in Storages :
    LET state == StorageState(i)
    IN \/ \E j \in Storages :
           /\ Len(state.networkReceive[j]) > 0
           /\ StorageStateApply(i, ProcessReplicationBucket(state, j))
       \/ \E b \in BucketIds :
            StorageStateApply(i, TestBucketStatusChange(state, b))

(***************************************************************************)
(*                      TEMPORAL PROPERTIES                                *)
(***************************************************************************)

\* Helper: check that two storages have matching bucket states
ReplicaStatesMatch(s1, s2) ==
    \A b \in BucketIds :
       storages[s1].buckets[b].status = storages[s2].buckets[b].status /\
       storages[s1].buckets[b].destination = storages[s2].buckets[b].destination

\* Helper: storages in a given replicaset
StoragesInReplicaset(rs) ==
    {s \in Storages : storageToReplicaset[s] = rs}

\* All replicas in a replicaset have consistent bucket states
AllReplicasConsistent ==
    \A rs \in ReplicaSets :
        \A s1, s2 \in StoragesInReplicaset(rs) :
            ReplicaStatesMatch(s1, s2)

NetworkEmpty ==
    \A s \in Storages : \A t \in Storages : network[s][t] = <<>>

\* Check if two storages have matching vclocks
VClockMatch(s1, s2) ==
    storages[s1].vclock = storages[s2].vclock

\* All storages in a replicaset have the same vclock
AllReplicasVClocksConsistent ==
    \A rs \in ReplicaSets :
        \A s1, s2 \in StoragesInReplicaset(rs) :
            VClockMatch(s1, s2)

\* AllReplicasConsistent cannot be used here: We cannot guarantee data
\* consistency, when there's master-master config, data depends on the
\* order of network messages processed.
ConsistencyWhenNetworkEmptyInv ==
    NetworkEmpty => (AllReplicasVClocksConsistent)

=============================================================================
