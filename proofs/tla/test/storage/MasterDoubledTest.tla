-------------------------- MODULE MasterDoubledTest ----------------------------
EXTENDS storage, TLC

(***************************************************************************)
(* Cluster: two replica sets, two storages each.                           *)
(* We'll force rs1 to send a bucket to rs2, lose replication, and failover.*)
(***************************************************************************)

CONSTANTS
  b1, b2, b3, b4

StoragesC == {"s1", "s2", "s3", "s4"}
ReplicaSetsC == {"rs1", "rs2"}
BucketIdsC == {b1, b2, b3, b4}
StorageAssignmentsC ==
    [rs1 |-> {"s1", "s2"},
     rs2 |-> {"s3", "s4"}]
BucketAssignmentsC ==
    [rs1 |-> {b1, b2},
     rs2 |-> {b3, b4}]
MasterAssignmentsC ==
    [rs1 |-> {"s1"},
     rs2 |-> {"s3"}]

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
     /\ \E i \in {"s1"}, j \in {"s3"}, b \in {b1} :
          \/ /\ Len(StorageState("s2").networkReceive[i]) > 0
             /\ StorageStateApply("s2", ProcessReplicationBucket(StorageState("s2"), i))
          \/ StorageStateApply(i, BucketSendStart(StorageState(i), b, j))
          \/ StorageStateApply(i, BucketSendWaitAndSend(StorageState(i), b, j))
          \/ /\ Len(StorageState(j).networkReceive[i]) > 0
             /\ \/ StorageStateApply(j, BucketRecvStart(StorageState(j), i))
                \/ StorageStateApply(j, BucketRecvFinish(StorageState(j), i))
          \/ /\ Len(StorageState(i).networkReceive[j]) > 0
             /\ StorageStateApply(i, BucketSendFinish(StorageState(i), j))
     /\ UNCHANGED <<storageToReplicaset, phase>>

  \/ /\ phase = 1
     /\ storages["s1"].buckets[b1].status = "SENT"
     /\ storages["s3"].buckets[b1].status = "ACTIVE"
     /\ UNCHANGED <<network, storages, storageToReplicaset>>
     /\ phase' = 3

  \/ /\ phase = 3
     /\ \E i \in {"s2"} :
          /\ StorageStateApply(i, BecomeMaster(StorageState(i)))
          /\ PrintT("Phase 3: failover, s2 becomes master")
     /\ phase' = 4

  \/ /\ phase = 4
     /\ UNCHANGED <<network, storages, storageToReplicaset, phase>>
     /\ PrintT("Phase 4: check for double ACTIVE")

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
