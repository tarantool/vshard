-------------------------------- MODULE StartTest ------------------------------

EXTENDS storage

CONSTANTS b1, b2, b3, b4, c1

StoragesC == {"s1", "s2", "s3", "s4"}
ReplicaSetsC == {"rs1", "rs2"}
BucketIdsC == {b1, b2, b3, b4}
StorageAssignmentsC == [rs1 |-> {"s1", "s2"},
                       rs2 |-> {"s3", "s4"}]
BucketAssignmentsC == [rs1 |-> {b1, b2},
                       rs2 |-> {b3, b4}]
MasterAssignmentsC == [rs1 |-> {"s1"},
                       rs2 |-> {"s3"}]

================================================================================
