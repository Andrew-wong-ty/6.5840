package shardctrler

import "fmt"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number (The shard controller manages a sequence of numbered configurations)
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) String() string {
	return fmt.Sprintf("Config{Num: %d, Shards: %v}", c.Num, convertToNewG2S(c.Shards))
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64            // the client who initiated the request
	SerialNum uint64           // prevent duplicate requests (always>=1)
	TransId   int64
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64  // the client who initiated the request
	SerialNum uint64 // prevent duplicate requests (always>=1)
	TransId   int64
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64  // the client who initiated the request
	SerialNum uint64 // prevent duplicate requests (always>=1)
	TransId   int64
}

type QueryArgs struct {
	Num       int    // desired config number
	ClientId  int64  // the client who initiated the request
	SerialNum uint64 // prevent duplicate requests (always>=1)
	TransId   int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
