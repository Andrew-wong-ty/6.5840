## Implementation of MIT-6.5840 lab 2,3 and 4 (23-spring)
- The code is pretty easy to understand and read, with detailed references from the [Raft Paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf). And it has no any confusing tricks, making it friendly to beginners of Golang and distributed systems. 
- All tests are run at least 100 times.
- Intergrated [this blog post about effective print statements](https://blog.josejg.com/debugging-pretty/) for easier debugging.

## Bugs Encountered in `ShardKV` lab

---
### Put/Append applied twice during shard migration
- **Case**: The `Put/Append` is applied on group 101, then the correspondent shard is migrated to 102, where the same `Put/Append` is applied again.
- **Solution**: each request should only have one sequence number, and the server/client should not update the seqNum when it fails to be executed for reasons
  such as `ErrWrongGroup` and `ErrShardNotReady`. The server should only update the client state (highest sequence number) when the `Put/Append` is successfully executed.
  And during the shard migration, copy the map of `clientID -> SequenceNumber` from one group to the target group of migration to prevent the same command from being executed twice in a different group.

---
### After servers restart, the group gets stuck waiting for shards
- **Case**: The code designs the server poll the next `Config` until all the shards it needs are installed. After the server group restarts, the
  whole group gets stuck waiting for shards that have been installed before the group is shut down. The following is each server's raft state when restarted:

|                             | S0                                | S1                                | S2     |
|-----------------------------|-----------------------------------|-----------------------------------|--------|
| rf.currentTerm              | 8                                 | 8                                 | 8      |
| rf.firstLogIdx              | 158                               | 158                               | 159    |
| rf.logs                     | [log0, log_to_install_shard(T=8)] | [log0, log_to_install_shard(T=8)] | [log0] |
| rf.commitIndex              | 157                               | 157                               | 158    |
| rf.lastApplied              | 157                               | 157                               | 158    |
| rf.snapshotLastIncludedIdx  | 157                               | 157                               | 158    |
| rf.snapshotLastIncludedTerm | 8                                 | 8                                 | 8      |

where S0 and S1 contain the log with the command to install shards, which are waiting for commit. And S2 applied this log and did a snapshot since it was the leader before
the whole group shut down.
After restarting the whole group, suppose
S0 is elected to be the leader. In this case, the `log_to_install_shard` in S0 and S1 will never be committed since the log's term is 8
but the leader's current term is 9 (8 +1 after the election).
So the group gets stuck in waiting for `log_to_install_shard` to be committed and applied.
- **Solution**: start an agreement on an empty log after leader's election. See section 8 of the raft paper "having each leader commit a blank no-op entry into the log at the start of its term".

---
### Server miss configuration changes
- **Solution**: do not poll the next config from shardctrler until all shards the server needs are migrated.


