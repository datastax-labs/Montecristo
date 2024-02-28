## Blocked flush writers

The Flush Writer thread pool is used to write Memtables to disk to create SSTables. When the queue for the thread pool is full, mutation threads (used for writes) will block until the disk activity has caught up. This will cause write latency to spike, as Coordinators will still be accepting mutations and sending messages to nodes.  


{{blockedFlushWritersSetting}}


Blocked flush writers may occur due to:

- Disk IO not keeping up with requirements.
- Incorrect configuration of the flush system.
- Data model edge cases where multiple Tables must flush at the same time due to commit log segment recycling.
- Use of nodetool flush or nodetool snapshot requiring multiple tables to flush at the same time.
- Blocked flush writers are found using the nodetool tpstats tool.


{{blockedTable}}
