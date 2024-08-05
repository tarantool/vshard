package vshard_router

import "fmt"

var nameToError = map[string]Error{}

// equal to https://github.com/tarantool/vshard/blob/master/vshard/error.lua
func init() {
	for _, err := range Errors {
		nameToError[err.Name] = err
	}
}

type Error struct {
	Name string
	Msg  string
	Args []string
}

func ErrorByName(name string) Error {
	return nameToError[name]
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s with args %s", e.Name, e.Msg, e.Args) // todo: fix errors wrapping
}

var Errors = map[int]Error{
	1: {
		Name: "WRONG_BUCKET",
		Msg:  "Cannot perform action with bucket %d, reason: %s",
		Args: []string{"bucket_id", "reason", "destination"},
	},
	2: {
		Name: "NON_MASTER",
		Msg:  "Replica %s is not a master for replicaset %s anymore",
		Args: []string{"replica", "replicaset", "master"},
	},
	3: {
		Name: "BUCKET_ALREADY_EXISTS",
		Msg:  "Bucket %d already exists",
		Args: []string{"bucket_id"},
	},
	4: {
		Name: "NO_SUCH_REPLICASET",
		Msg:  "Replicaset %s not found",
		Args: []string{"replicaset"},
	},
	5: {
		Name: "MOVE_TO_SELF",
		Msg:  "Cannot move: bucket %d is already on replicaset %s",
		Args: []string{"bucket_id", "replicaset"},
	},
	6: {
		Name: "MISSING_MASTER",
		Msg:  "Master is not configured for replicaset %s",
		Args: []string{"replicaset"},
	},
	7: {
		Name: "TRANSFER_IS_IN_PROGRESS",
		Msg:  "Bucket %d is transferring to replicaset %s",
		Args: []string{"bucket_id", "destination"},
	},
	8: {
		Name: "UNREACHABLE_REPLICASET",
		Msg:  "There is no active replicas in replicaset %s",
		Args: []string{"replicaset", "bucket_id"},
	},
	9: {
		Name: "NO_ROUTE_TO_BUCKET",
		Msg:  "Bucket %d cannot be found. Is rebalancing in progress?",
		Args: []string{"bucket_id"},
	},
	10: {
		Name: "NON_EMPTY",
		Msg:  "Cluster is already bootstrapped",
	},
	11: {
		Name: "UNREACHABLE_MASTER",
		Msg:  "Master of replicaset %s is unreachable: %s",
		Args: []string{"replicaset", "reason"},
	},
	12: {
		Name: "OUT_OF_SYNC",
		Msg:  "Replica is out of sync",
	},
	13: {
		Name: "HIGH_REPLICATION_LAG",
		Msg:  "High replication lag: %f",
		Args: []string{"lag"},
	},
	14: {
		Name: "UNREACHABLE_REPLICA",
		Msg:  "Replica %s isn't active",
		Args: []string{"replica"},
	},
	15: {
		Name: "LOW_REDUNDANCY",
		Msg:  "Only one replica is active",
	},
	16: {
		Name: "INVALID_REBALANCING",
		Msg:  "Sending and receiving buckets at same time is not allowed",
	},
	17: {
		Name: "SUBOPTIMAL_REPLICA",
		Msg:  "A current read replica in replicaset %s is not optimal",
		Args: []string{"replicaset"},
	},
	18: {
		Name: "UNKNOWN_BUCKETS",
		Msg:  "%d buckets are not discovered",
		Args: []string{"not_discovered_cnt"},
	},
	19: {
		Name: "REPLICASET_IS_LOCKED",
		Msg:  "Replicaset is locked",
	},
	20: {
		Name: "OBJECT_IS_OUTDATED",
		Msg:  "Object is outdated after module reload/reconfigure. Use new instance.",
	},
	21: {
		Name: "ROUTER_ALREADY_EXISTS",
		Msg:  "Router with name %s already exists",
		Args: []string{"router_name"},
	},
	22: {
		Name: "BUCKET_IS_LOCKED",
		Msg:  "Bucket %d is locked",
		Args: []string{"bucket_id"},
	},
	23: {
		Name: "INVALID_CFG",
		Msg:  "Invalid configuration: %s",
		Args: []string{"reason"},
	},
	24: {
		Name: "BUCKET_IS_PINNED",
		Msg:  "Bucket %d is pinned",
		Args: []string{"bucket_id"},
	},
	25: {
		Name: "TOO_MANY_RECEIVING",
		Msg:  "Too many receiving buckets at once, please, throttle",
	},
	26: {
		Name: "STORAGE_IS_REFERENCED",
		Msg:  "Storage is referenced",
	},
	27: {
		Name: "STORAGE_REF_ADD",
		Msg:  "Can not add a storage ref: %s",
		Args: []string{"reason"},
	},
	28: {
		Name: "STORAGE_REF_USE",
		Msg:  "Can not use a storage ref: %s",
		Args: []string{"reason"},
	},
	29: {
		Name: "STORAGE_REF_DEL",
		Msg:  "Can not delete a storage ref: %s",
		Args: []string{"reason"},
	},
	30: {
		Name: "BUCKET_RECV_DATA_ERROR",
		Msg:  "Can not receive the bucket %s data in space \"%s\" at tuple %s: %s",
		Args: []string{"bucket_id", "space", "tuple", "reason"},
	},
	31: {
		Name: "MULTIPLE_MASTERS_FOUND",
		Msg:  "Found more than one master in replicaset %s on nodes %s and %s",
		Args: []string{"replicaset", "master1", "master2"},
	},
	32: {
		Name: "REPLICASET_IN_BACKOFF",
		Msg:  "Replicaset %s is in backoff, can't take requests right now. Last error was %s",
		Args: []string{"replicaset", "error"},
	},
	33: {
		Name: "STORAGE_IS_DISABLED",
		Msg:  "Storage is disabled: %s",
		Args: []string{"reason"},
	},
	34: {
		Name: "BUCKET_IS_CORRUPTED",
		Msg:  "Bucket %d is corrupted: %s",
		Args: []string{"bucket_id", "reason"},
	},
	35: {
		Name: "ROUTER_IS_DISABLED",
		Msg:  "Router is disabled: %s",
		Args: []string{"reason"},
	},
	36: {
		Name: "BUCKET_GC_ERROR",
		Msg:  "Error during bucket GC: %s",
		Args: []string{"reason"},
	},
	37: {
		Name: "STORAGE_CFG_IS_IN_PROGRESS",
		Msg:  "Configuration of the storage is in progress",
	},
	38: {
		Name: "ROUTER_CFG_IS_IN_PROGRESS",
		Msg:  "Configuration of the router with name %s is in progress",
		Args: []string{"router_name"},
	},
	39: {
		Name: "BUCKET_INVALID_UPDATE",
		Msg:  "Bucket %s update is invalid: %s",
		Args: []string{"bucket_id", "reason"},
	},
	40: {
		Name: "VHANDSHAKE_NOT_COMPLETE",
		Msg:  "Handshake with %s have not been completed yet",
		Args: []string{"replica"},
	},
	41: {
		Name: "INSTANCE_NAME_MISMATCH",
		Msg:  "Mismatch server name: expected \"%s\", but got \"%s\"",
		Args: []string{"expected_name", "actual_name"},
	},
}

type BucketStatError struct {
	BucketID uint64 `msgpack:"bucket_id"`
	Reason   string `msgpack:"reason"`
	Code     int    `msgpack:"code"`
	Type     string `msgpack:"type"`
	Message  string `msgpack:"message"`
	Name     string `msgpack:"name"`
}

func (bse BucketStatError) Error() string {
	return "todo"
}
