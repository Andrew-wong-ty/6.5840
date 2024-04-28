package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
)

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
	return w.Bytes()
}

func (kv *ShardKV) decodeSnapshot(snapshot []byte) (map[int64]uint64, map[string]string, shardctrler.Config) {
	if snapshot == nil || len(snapshot) < 1 {
		panic("empty snapshot")
	}
	var clt2SerialNum map[int64]uint64
	var db map[string]string
	var latestCfg shardctrler.Config
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
	err = d.Decode(&latestCfg)
	if err != nil {
		panic("decode error")
	}
	return clt2SerialNum, db, latestCfg
}
