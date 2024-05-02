package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
)

func (kv *ShardKV) decodeAndInstallSnapshot(snapBytes []byte, snapIdx int) {
	clt2SerialNum, db, currCfg, prevCfg, clientId, lastApplied := kv.decodeSnapshot(snapBytes)
	if snapIdx >= kv.lastApplied {
		kv.lastApplied = snapIdx
		kv.clientId2SerialNum = clt2SerialNum
		kv.inMemoryDB = db
		kv.currCfg = currCfg
		kv.prevCfg = prevCfg
		kv.clientId = clientId
		kv.lastApplied = lastApplied
	}
}

// ! Note: be careful about if gob knows the type to be encoded
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.clientId2SerialNum)
	if err != nil {
		panic("encode error")
	}

	err = e.Encode(kv.inMemoryDB)
	if err != nil {
		panic("encode error")
	}

	err = e.Encode(kv.currCfg)
	if err != nil {
		panic("encode error")
	}

	err = e.Encode(kv.prevCfg)
	if err != nil {
		panic("encode error")
	}

	err = e.Encode(kv.clientId)
	if err != nil {
		panic("encode error")
	}

	err = e.Encode(kv.lastApplied)
	if err != nil {
		panic("encode error")
	}

	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(snapshot []byte) (map[int64]uint64, [shardctrler.NShards]Shard, shardctrler.Config, shardctrler.Config, int64, int) {
	if snapshot == nil || len(snapshot) < 1 {
		panic("empty snapshot")
	}
	var clt2SerialNum map[int64]uint64
	var db [shardctrler.NShards]Shard
	var currCfg shardctrler.Config
	var prevCfg shardctrler.Config
	var clientId int64
	var lastApplied int
	//var lastApplied int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	err := d.Decode(&clt2SerialNum)
	if err != nil {
		panic("decode error")
	}
	err = d.Decode(&db)
	if err != nil {
		panic("decode error")
	}
	err = d.Decode(&currCfg)
	if err != nil {
		panic("decode error")
	}
	err = d.Decode(&prevCfg)
	if err != nil {
		panic("decode error")
	}
	err = d.Decode(&clientId)
	if err != nil {
		panic("decode error")
	}
	err = d.Decode(&lastApplied)
	if err != nil {
		panic("decode error")
	}
	return clt2SerialNum, db, currCfg, prevCfg, clientId, lastApplied
}
