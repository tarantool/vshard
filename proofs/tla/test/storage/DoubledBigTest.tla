--------------------------- MODULE DoubledBigTest ------------------------------
EXTENDS storage, utils

CONSTANTS b1, b2, b3, b4

StoragesC == {"s1", "s2", "s3", "s4"}
ReplicaSetsC == {"rs1", "rs2", "rs3"}
BucketIdsC == {b1, b2, b3, b4}
StorageAssignmentsC == [rs1 |-> {"s1", "s2"},
                       rs2 |-> {"s3"},
                       rs3 |-> {"s4"}]
BucketAssignmentsC == [rs1 |-> {b1},
                       rs2 |-> {b2},
                       rs3 |-> {b3, b4}]
MasterAssignmentsC == [rs1 |-> {"s1"},
                       rs2 |-> {"s3"},
                       rs3 |-> {"s4"}]

(***************************************************************************)
(*                           CONSTRAINTS                                   *)
(***************************************************************************)

MAX_TOTAL_SENDS == 3

\* 1. Limit total bucket sends - prevent endless transfers.
SendLimitConstraint ==
    LET totalSends ==
        SetSum({ storages[i].errinj.bucketSendCount : i \in StoragesC })
    IN totalSends =< MAX_TOTAL_SENDS

\* 2. Keep network bounded - avoid message explosion.
NetworkBoundConstraint ==
    /\ \A s1, s2 \in StoragesC :
            Len(network[s1][s2]) =< 3
    /\ \A s \in StoragesC :
        /\ storages[s].errinj.networkReorderCount <= 2
        /\ storages[s].errinj.networkDropCount <= 2

RefConstraint ==
    \A s1 \in StoragesC :
        /\ storages[s1].errinj.bucketRWRefCount <= 1
        /\ storages[s1].errinj.bucketRORefCount <= 1
        /\ storages[s1].errinj.bucketRWUnRefCount <= 1
        /\ storages[s1].errinj.bucketROUnRefCount <= 1

TransitionConstraint ==
    \A s1 \in StoragesC :
        /\ storages[s1].errinj.masterTransitionCount <= 1
        /\ storages[s1].errinj.replicaTransitionCount <= 1

(***************************************************************************)
(*                            SYMMETRY                                     *)
(***************************************************************************)

Symmetry ==
    Permutations(BucketIdsC)

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

================================================================================
