-------------------------MODULE DoubledBucketsSmallTest ------------------------
EXTENDS storage, utils

CONSTANTS b1, b2

StoragesC == {"s1", "s2", "s3", "s4"}
ReplicaSetsC == {"rs1", "rs2"}
BucketIdsC == {b1, b2}
StorageAssignmentsC == [rs1 |-> {"s1", "s2"},
                       rs2 |-> {"s3", "s4"}]
BucketAssignmentsC == [rs1 |-> {b1},
                       rs2 |-> {b2}]
MasterAssignmentsC == [rs1 |-> {"s1"},
                       rs2 |-> {"s3"}]

(***************************************************************************)
(*                           CONSTRAINTS                                   *)
(***************************************************************************)

MAX_TOTAL_SENDS == 1

\* 1. Limit total bucket sends - prevent endless transfers.
SendLimitConstraint ==
    LET totalSends ==
        SetSum({ storages[i].errinj.bucketSendCount : i \in Storages })
    IN totalSends =< MAX_TOTAL_SENDS

\* 2. Allow only a small number of concurrent masters.
TwoMastersConstraint ==
    Cardinality({s \in Storages : storages[s].status = "master"}) =< 2

\* 3. Keep network bounded - avoid message explosion.
NetworkBoundConstraint ==
    \A s1, s2 \in StoragesC :
        Len(network[s1][s2]) =< 2

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
