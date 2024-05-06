package shardkv

import "fmt"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType                 string // GET or PUT or APPEND or INSTALLSHARD or UPDATECONFIG or DELETESHARD
	Key                    string // for GET, PUT, APPEND
	Value                  string // for GET
	ResultForGet           string // for GET; the result for get
	ShardData              string // for INSTALLSHARD; (gob+base64) serialized string
	ShardIDs               string // for INSTALLSHARD; the shard ids to be installed
	ShardDataVersion       int    // for INSTALLSHARD; the shardData's cfg.Num
	InstalledSuccessShards string // for INSTALLSHARD;
	Version                int    // for UPDATECONFIG; the version of the new shard data
	AddedShards            string // for UPDATECONFIG; the shard ids to be added (gob+base64)
	NewConfig              string // for UPDATECONFIG; the new config (gob+base64)
	Client2SerialNum       string // for INSTALLSHARD
	SerialNum              uint64
	ClientId               int64
	Error                  Err
}

func (op *Op) String() string {
	res := fmt.Sprintf("key=%v, value=%v, opType=%v, seqNum=%v, clientId=%v",
		op.Key, op.Value, op.OpType, op.SerialNum, op.ClientId)
	return res
}
