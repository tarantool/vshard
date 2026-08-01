------------------------------- MODULE utils -----------------------------------

EXTENDS Naturals, FiniteSets

RECURSIVE SetSum(_)
SetSum(set) == IF set = {} THEN 0 ELSE
  LET x == CHOOSE x \in set: TRUE
    IN x + SetSum(set \ {x})

================================================================================
