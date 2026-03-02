-------------------------- MODULE RecoveryGarbageTest --------------------------

\* I didn't manage to end that test in 1 hour and after 150 mln states.
\* Need much more constraints for proper execution time. But it helped me to find
\* a ton of bugs and it works, for now I'm ok with this for now.

EXTENDS storage, utils

(***************************************************************************)
(* Cluster setup: 2 replica sets, 1 storage each, 2 buckets per set.       *)
(***************************************************************************)

CONSTANTS
  b1, b2, b3, b4

StoragesC == {"s1", "s2"}
ReplicaSetsC == {"rs1", "rs2"}
BucketIdsC == {b1, b2, b3, b4}
StorageAssignmentsC ==
    [rs1 |-> {"s1"},
     rs2 |-> {"s2"}]
BucketAssignmentsC ==
    [rs1 |-> {b1, b2},
     rs2 |-> {b3, b4}]
MasterAssignmentsC ==
    [rs1 |-> {"s1"},
     rs2 |-> {"s2"}]

(***************************************************************************)
(*                           CONSTRAINTS                                   *)
(***************************************************************************)

MAX_TOTAL_SENDS == 2

\* 1. Limit total bucket sends - prevent endless transfers.
SendLimitConstraint ==
    LET totalSends ==
        SetSum({ storages[i].errinj.bucketSendCount : i \in Storages })
    IN totalSends =< MAX_TOTAL_SENDS

(***************************************************************************)
(* Helpers                                                                 *)
(***************************************************************************)

DropFinalRecvMessage ==
  \E s1, s2 \in StoragesC :
    /\ Len(network[s1][s2]) > 0
    /\ Head(network[s1][s2]).type = "BUCKET_RECV"
    /\ Head(network[s1][s2]).content.final = TRUE
    /\ network' = [network EXCEPT ![s1][s2] = Tail(@)]
    /\ UNCHANGED <<storages, storageToReplicaset>>

(***************************************************************************)
(* Transitions                                                             *)
(***************************************************************************)

NextTest ==
  \E i \in StoragesC :
    LET st == StorageState(i) IN
    \/ \E j \in StoragesC, b \in BucketIdsC :
        StorageStateApply(i, BucketSendStart(st, b, j))
    \/ \E j \in StoragesC :
        /\ Len(st.networkReceive[j]) > 0
        /\ \/ StorageStateApply(i, BucketRecvStart(st, j))
           \/ StorageStateApply(i, BucketSendFinish(st, j))
           \/ StorageStateApply(i, BucketRecvFinish(st, j))
           \/ StorageStateApply(i, ProcessRecoveryStatRequest(st, j))
           \/ StorageStateApply(i, ProcessRecoveryStatResponse(st, j))
           \/ StorageStateApply(i, ProcessGcTestRequest(st, j))
           \/ StorageStateApply(i, ProcessGcTestResponse(st, j))
    \/ \E b \in BucketIdsC :
        \/ StorageStateApply(i, RecoverySendStatRequest(st, b))
        \/ StorageStateApply(i, GcDropGarbage(st, b))
        \/ StorageStateApply(i, GcSendTestRequest(st, b))
    \/ DropFinalRecvMessage
    \/ UNCHANGED <<network, storages, storageToReplicaset>>

(***************************************************************************)
(* Specification                                                           *)
(***************************************************************************)

Spec ==
  Init /\ [][NextTest]_<<network, storages, storageToReplicaset>> /\
  WF_<<network, storages, storageToReplicaset>>(NextTest)

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

AllBucketsEventuallyStable ==
  [] (\A s \in StoragesC, b \in BucketIdsC :
        <> (storages[s].buckets[b].status \in {"ACTIVE", NULL}))

NoActiveSimultaneousInv ==
    \* No bucket can be ACTIVE in storages belonging to different ReplicaSets
    \A b \in BucketIds :
        \A rs1, rs2 \in ReplicaSets :
            rs1 # rs2 =>
                ~(\E s1, s2 \in Storages :
                     storageToReplicaset[s1] = rs1 /\
                     storageToReplicaset[s2] = rs2 /\
                     storages[s1].status = "master" /\
                     storages[s2].status = "master" /\
                     storages[s1].buckets[b].status = "ACTIVE" /\
                     storages[s2].buckets[b].status = "ACTIVE")

===============================================================================
