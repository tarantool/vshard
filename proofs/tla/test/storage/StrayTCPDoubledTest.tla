------------------------- MODULE StrayTCPDoubledTest ---------------------------
EXTENDS storage, TLC

CONSTANTS
  b1, b2, b3

StoragesC == {"s1", "s2", "s3"}
ReplicaSetsC == {"rs1", "rs2", "rs3"}
BucketIdsC == {b1, b2, b3}
StorageAssignmentsC ==
    [rs1 |-> {"s1"},
     rs2 |-> {"s2"},
     rs3 |-> {"s3"}]
BucketAssignmentsC ==
    [rs1 |-> {b1},
     rs2 |-> {b2},
     rs3 |-> {b3}]
MasterAssignmentsC ==
    [rs1 |-> {"s1"},
     rs2 |-> {"s2"},
     rs3 |-> {"s3"}]

(***************************************************************************)
(* Variables and initialization                                            *)
(***************************************************************************)

VARIABLE phase

TestInit ==
  /\ Init
  /\ phase = 1

(***************************************************************************)
(*                           CONSTRAINTS                                   *)
(***************************************************************************)

NetworkReorderConstraint ==
    \A s \in StoragesC :
        /\ storages[s].errinj.networkReorderCount <= 1
        /\ storages[s].errinj.networkDropCount <= 1

(***************************************************************************)
(* Phase-driven Next                                                       *)
(***************************************************************************)

TestNext ==
  \/ /\ phase = 1
     /\ \E i \in {"s1"}, j \in {"s3"}, b \in {b1} :
          StorageStateApply(i, BucketSendStart(StorageState(i), b, j))
     /\ phase' = 2
     /\ UNCHANGED <<storageToReplicaset>>

  \/ /\ phase = 2
     /\ storages["s1"].buckets[b1].status = "SENDING"
     /\ StorageStateApply("s1", BucketDropFromTransfering(StorageState("s1"), b1))
     /\ phase' = 3
     /\ UNCHANGED <<storageToReplicaset>>

  \/ /\ phase = 3
     /\ storages["s1"].buckets[b1].status = "SENDING"
     /\ \E i \in {"s1"}, j \in {"s3"}, b \in {b1} :
        \/ StorageStateApply(i, RecoverySendStatRequest(StorageState(i), b))
        \/ /\ Len(StorageState(j).networkReceive[i]) > 0
           /\ StorageStateApply(j, ProcessRecoveryStatRequest(StorageState(j), i))
        \/ /\ Len(StorageState(i).networkReceive[j]) > 0
           /\ StorageStateApply(i, ProcessRecoveryStatResponse(StorageState(i), j))
        \/ ReorderOneNetworkMessage
     /\ UNCHANGED <<storageToReplicaset, phase>>

  \/ /\ phase = 3
     /\ storages["s1"].buckets[b1].status = "ACTIVE"
     /\ Head(network["s1"]["s3"]).type = "BUCKET_RECV"
     /\ phase' = 4
     /\ UNCHANGED <<network, storages, storageToReplicaset>>

  \/ /\ phase = 4
     /\ \E i \in {"s1"}, j \in {"s2"}, b \in {b1} :
          \/ StorageStateApply(i, BucketSendStart(StorageState(i), b, j))
          \/ /\ Len(StorageState(j).networkReceive[i]) > 0
             /\ \/ StorageStateApply(j, BucketRecvStart(StorageState(j), i))
                \/ StorageStateApply(j, BucketRecvFinish(StorageState(j), i))
          \/ /\ Len(StorageState(i).networkReceive[j]) > 0
             /\ StorageStateApply(i, BucketSendFinish(StorageState(i), j))
     /\ UNCHANGED <<storageToReplicaset, phase>>

  \/ /\ phase = 4
     /\ storages["s1"].buckets[b1].status = "SENT"
     /\ storages["s2"].buckets[b1].status = "ACTIVE"
     /\ storages["s3"].buckets[b1].status = NULL
     /\ phase' = 5
     /\ UNCHANGED <<network, storages, storageToReplicaset>>

  \/ /\ phase = 5
     /\ StorageStateApply("s1", GcSendTestRequest(StorageState("s1"), b1))
     /\ phase' = 6

  \/ /\ phase = 6
     /\ storages["s1"].buckets[b1].status = "GARBAGE"
     /\ storages["s2"].buckets[b1].status = "ACTIVE"
     /\ storages["s3"].buckets[b1].status = NULL
     /\ StorageStateApply("s3", BucketRecvStart(StorageState("s3"), "s1"))
     /\ phase' = 7

  \/ /\ phase = 7
     /\ storages["s1"].buckets[b1].status = "GARBAGE"
     /\ storages["s2"].buckets[b1].status = "ACTIVE"
     /\ storages["s3"].buckets[b1].status = "RECEIVING"
     /\ phase' = 8
     /\ UNCHANGED <<network, storages, storageToReplicaset>>

  \/ /\ phase = 8
     /\ \E i \in {"s3"}, j \in {"s1"}, b \in {b1} :
        \/ DropOneNetworkMessage
        \/ StorageStateApply(i, RecoverySendStatRequest(StorageState(i), b))
        \/ /\ Len(StorageState(j).networkReceive[i]) > 0
           /\ StorageStateApply(j, ProcessRecoveryStatRequest(StorageState(j), i))
        \/ /\ Len(StorageState(i).networkReceive[j]) > 0
           /\ StorageStateApply(i, ProcessRecoveryStatResponse(StorageState(i), j))
     /\ UNCHANGED <<phase>>

  \/ /\ phase = 8
     /\ storages["s1"].buckets[b1].status = "GARBAGE"
     /\ storages["s2"].buckets[b1].status = "ACTIVE"
     /\ storages["s3"].buckets[b1].status = "ACTIVE"
     /\ phase' = 9
     /\ UNCHANGED <<network, storages, storageToReplicaset>>

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
