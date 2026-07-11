----------------------------- MODULE RecoveryTest ------------------------------
EXTENDS storage, TLC

(***************************************************************************)
(* Cluster: two replica sets, two storages each.                           *)
(* We'll force rs1 to send a bucket to rs2, lose replication, and failover.*)
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
(* Variables and initialization                                            *)
(***************************************************************************)

VARIABLE phase

TestInit ==
  /\ Init
  /\ phase = 1

(***************************************************************************)
(* Phase-driven Next                                                       *)
(***************************************************************************)

TestNext ==
  \/ /\ phase = 1
     /\ \E i \in {"s1"}, j \in {"s2"}, b \in {b1} :
            /\ StorageStateApply(i, BucketSendStart(StorageState(i), b, j))
            \* /\ PrintT("Phase 1: bucket_send()")
     /\ phase' = 2

  \/ /\ phase = 2
     /\ storages["s1"].buckets[b1].status = "SENDING"
     /\ storages["s2"].buckets[b1].status = NULL
     /\ \E i \in {"s1"}, b \in {b1} :
            StorageStateApply(i, BucketDropFromTransfering(StorageState(i), b))
     \* /\ PrintT("Phase 2: timeout bucket_send()")
     /\ phase' = 3

  \/ /\ phase = 3
     /\ \E i \in {"s1"}, j \in {"s2"} :
            /\ network' = [network EXCEPT ![i][j] = Tail(@)]
            /\ UNCHANGED <<storages, storageToReplicaset>>
     \* /\ PrintT("Phase 3: timeout bucket_send()")
     /\ phase' = 4

  \/ /\ phase = 4
     /\ \E i \in {"s1"}, j \in {"s2"}, b \in {b1} :
        \/ StorageStateApply(i, RecoverySendStatRequest(StorageState(i), b))
        \/ /\ Len(StorageState(j).networkReceive[i]) > 0
           /\ StorageStateApply(j, ProcessRecoveryStatRequest(StorageState(j), i))
        \/ /\ Len(StorageState(i).networkReceive[j]) > 0
           /\ StorageStateApply(i, ProcessRecoveryStatResponse(StorageState(i), j))
     \* /\ PrintT("Phase 4: recover bucket")
     /\ UNCHANGED <<phase>>

  \/ /\ phase = 4
     /\ storages["s1"].buckets[b1].status = "ACTIVE"
     /\ storages["s2"].buckets[b1].status = NULL
     /\ phase' = 5
     /\ UNCHANGED <<network, storages, storageToReplicaset>>
     \* /\ PrintT("Phase 5: stutter")

(***************************************************************************)
(* Spec                                                                    *)
(***************************************************************************)

Spec ==
  TestInit /\ [][TestNext]_<<network, storages, storageToReplicaset, phase>>

(***************************************************************************)
(*                           STATE INVARIANTS                              *)
(***************************************************************************)

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
