# BookKeeper

BookKeeper is a replicated log storage system that Pulsar uses for durable storage of all messages.

### bookiePort

The port on which the bookie server listens.

**Default**: 3181

### allowLoopback

Whether the bookie is allowed to use a loopback interface as its primary interface (that is the interface used to establish its identity). By default, loopback interfaces are not allowed to work as the primary interface. Using a loopback interface as the primary interface usually indicates a configuration error. For example, it’s fairly common in some VPS setups to not configure a hostname or to have the hostname resolve to `127.0.0.1`. If this is the case, then all bookies in the cluster will establish their identities as `127.0.0.1:3181` and only one will be able to join the cluster. For VPSs configured like this, you should explicitly set the listening interface.

**Default**: false

### listeningInterface

The network interface on which the bookie listens. By default, the bookie listens on all interfaces.

**Default**: eth0

### advertisedAddress

Configure a specific hostname or IP address that the bookie should use to advertise itself to clients. By default, the bookie advertises either its own IP address or hostname according to the `listeningInterface` and `useHostNameAsBookieID` settings.

**Default**: N/A

### allowMultipleDirsUnderSameDiskPartition

Configure the bookie to enable/disable multiple ledger/index/journal directories in the same filesystem disk partition.

**Default**: false

### minUsableSizeForIndexFileCreation

The minimum safe usable size available in index directory for bookie to create index files while replaying journal at the time of bookie starts in Readonly Mode (in bytes).

**Default**: 1073741824

### journalDirectory

The directory where BookKeeper outputs its write-ahead log (WAL).

**Default**: data/bookkeeper/journal

### journalDirectories

Directories that BookKeeper outputs its write ahead log. Multiple directories are available, being separated by `,`. For example: `journalDirectories=/tmp/bk-journal1,/tmp/bk-journal2`. If `journalDirectories` is set, the bookies skip `journalDirectory` and use this setting directory.

**Default**: /tmp/bk-journal

### ledgerDirectories

The directory where BookKeeper outputs ledger snapshots. This could define multiple directories to store snapshots separated by `,`, for example `ledgerDirectories=/tmp/bk1-data,/tmp/bk2-data`. Ideally, ledger dirs and the journal dir are each in a different device, which reduces the contention between random I/O and sequential write. It is possible to run with a single disk, but performance will be significantly lower.

**Default**: data/bookkeeper/ledgers

### ledgerManagerType

The type of ledger manager used to manage how ledgers are stored, managed, and garbage collected. See [BookKeeper Internals](https://bookkeeper.apache.org/docs/next/getting-started/concepts) for more info.

**Default**: hierarchical

### zkLedgersRootPath

The root ZooKeeper path used to store ledger metadata. This parameter is used by the ZooKeeper-based ledger manager as a root znode to store all ledgers.

**Default**: /ledgers

### ledgerStorageClass

Ledger storage implementation class

**Default**: org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage

### entryLogFilePreallocationEnabled

Enable or disable entry logger preallocation

**Default**: true

### logSizeLimit

Max file size of the entry logger, in bytes. A new entry log file will be created when the old one reaches the file size limitation.

**Default**: 2147483648

### minorCompactionThreshold

Threshold of minor compaction. Entry log files whose remaining size percentage reaches below this threshold will be compacted in a minor compaction. If set to less than zero, the minor compaction is disabled.

**Default**: 0.2

### minorCompactionInterval

Time interval to run minor compaction, in seconds. If set to less than zero, the minor compaction is disabled. Note: should be greater than gcWaitTime.

**Default**: 3600

### majorCompactionThreshold

The threshold of major compaction. Entry log files whose remaining size percentage reaches below this threshold will be compacted in a major compaction. Those entry log files whose remaining size percentage is still higher than the threshold will never be compacted. If set to less than zero, the major compaction is disabled.

**Default**: 0.5

### majorCompactionInterval

The time interval to run major compaction, in seconds. If set to less than zero, the major compaction is disabled. Note: should be greater than gcWaitTime.

**Default**: 86400

### readOnlyModeEnabled

If `readOnlyModeEnabled=true`, then on all full ledger disks, bookie will be converted to read-only mode and serve only read requests. Otherwise the bookie will be shutdown.

**Default**: true

### forceReadOnlyBookie

Whether the bookie is force started in read only mode.

**Default**: false

### persistBookieStatusEnabled

Persist the bookie status locally on the disks. So the bookies can keep their status upon restarts.

**Default**: false

### compactionMaxOutstandingRequests

Sets the maximum number of entries that can be compacted without flushing. When compacting, the entries are written to the entrylog and the new offsets are cached in memory. Once the entrylog is flushed the index is updated with the new offsets. This parameter controls the number of entries added to the entrylog before a flush is forced. A higher value for this parameter means more memory will be used for offsets. Each offset consists of 3 longs. This parameter should not be modified unless you’re fully aware of the consequences.

**Default**: 100000

### compactionRate

The rate at which compaction will read entries, in adds per second.

**Default**: 1000

### isThrottleByBytes

Throttle compaction by bytes or by entries.

**Default**: false

### compactionRateByEntries

The rate at which compaction will read entries, in adds per second.

**Default**: 1000

### compactionRateByBytes

Set the rate at which compaction reads entries. The unit is bytes added per second.

**Default**: 1000000

### journalMaxSizeMB

Max file size of journal file, in megabytes. A new journal file will be created when the old one reaches the file size limitation.

**Default**: 2048

### journalMaxBackups

The max number of old journal files to keep. Keeping a number of old journal files would help data recovery in special cases.

**Default**: 5

### journalPreAllocSizeMB

How space to pre-allocate at a time in the journal.

**Default**: 16

### journalWriteBufferSizeKB

The of the write buffers used for the journal.

**Default**: 64

### journalRemoveFromPageCache

Whether pages should be removed from the page cache after force write.

**Default**: true

### journalAdaptiveGroupWrites

Whether to group journal force writes, which optimizes group commit for higher throughput.

**Default**: true

### journalMaxGroupWaitMSec

The maximum latency to impose on a journal write to achieve grouping.

**Default**: 1

### journalAlignmentSize

All the journal writes and commits should be aligned to given size

**Default**: 4096

### journalBufferedWritesThreshold

Maximum writes to buffer to achieve grouping

**Default**: 524288

### journalFlushWhenQueueEmpty

If we should flush the journal when journal queue is empty

**Default**: false

### numJournalCallbackThreads

The number of threads that should handle journal callbacks

**Default**: 8

### openLedgerRereplicationGracePeriod

The grace period, in milliseconds, that the replication worker waits before fencing and replicating a ledger fragment that's still being written to upon bookie failure.

**Default**: 30000

### rereplicationEntryBatchSize

The number of max entries to keep in fragment for re-replication

**Default**: 100

### autoRecoveryDaemonEnabled

Whether the bookie itself can start auto-recovery service.

**Default**: true

### lostBookieRecoveryDelay

How long to wait, in seconds, before starting auto recovery of a lost bookie.

**Default**: 0

### gcWaitTime

How long the interval to trigger next garbage collection, in milliseconds. Since garbage collection is running in background, too frequent gc will heart performance. It is better to give a higher number of gc interval if there is enough disk capacity.

**Default**: 900000

### gcOverreplicatedLedgerWaitTime

How long the interval to trigger next garbage collection of overreplicated ledgers, in milliseconds. This should not be run very frequently since we read the metadata for all the ledgers on the bookie from zk.

**Default**: 86400000

### flushInterval

How long the interval to flush ledger index pages to disk, in milliseconds. Flushing index files will introduce much random disk I/O. If separating journal dir and ledger dirs each on different devices, flushing would not affect performance. But if putting journal dir and ledger dirs on same device, performance degrade significantly on too frequent flushing. You can consider increment flush interval to get better performance, but you need to pay more time on bookie server restart after failure.

**Default**: 60000

### bookieDeathWatchInterval

Interval to watch whether bookie is dead or not, in milliseconds

**Default**: 1000

### allowStorageExpansion

Allow the bookie storage to expand. Newly added ledger and index dirs must be empty.

**Default**: false

### zkServers

A list of one of more servers on which zookeeper is running. The server list can be comma separated values, for example: zkServers=zk1:2181,zk2:2181,zk3:2181.

**Default**: localhost:2181

### zkTimeout

ZooKeeper client session timeout in milliseconds Bookie server will exit if it received SESSION_EXPIRED because it was partitioned off from ZooKeeper for more than the session timeout JVM garbage collection, disk I/O will cause SESSION_EXPIRED. Increment this value could help avoiding this issue

**Default**: 30000

### zkRetryBackoffStartMs

The start time that the Zookeeper client backoff retries in milliseconds.

**Default**: 1000

### zkRetryBackoffMaxMs

The maximum time that the Zookeeper client backoff retries in milliseconds.

**Default**: 10000

### zkEnableSecurity

Set ACLs on every node written on ZooKeeper, allowing users to read and write BookKeeper metadata stored on ZooKeeper. In order to make ACLs work you need to setup ZooKeeper JAAS authentication. All the bookies and Client need to share the same user, and this is usually done using Kerberos authentication. See ZooKeeper documentation.

**Default**: false

### httpServerEnabled

The flag enables/disables starting the admin http server.

**Default**: false

### httpServerPort

The HTTP server port to listen on. By default, the value is `8080`. If you want to keep it consistent with the Prometheus stats provider, you can set it to `8000`.

**Default**: 8080

### httpServerClass

The http server class.

**Default**: org.apache.bookkeeper.http.vertx.VertxHttpServer

### serverTcpNoDelay

This settings is used to enabled/disabled Nagle’s algorithm, which is a means of improving the efficiency of TCP/IP networks by reducing the number of packets that need to be sent over the network. If you are sending many small messages, such that more than one can fit in a single IP packet, setting server.tcpnodelay to false to enable Nagle algorithm can provide better performance.

**Default**: true

### serverSockKeepalive

This setting is used to send keep-alive messages on connection-oriented sockets.

**Default**: true

### serverTcpLinger

The socket linger timeout on close. When enabled, a close or shutdown will not return until all queued messages for the socket have been successfully sent or the linger timeout has been reached. Otherwise, the call returns immediately and the closing is done in the background.

**Default**: 0

### byteBufAllocatorSizeMax

The maximum buf size of the received ByteBuf allocator.

**Default**: 1048576

### nettyMaxFrameSizeBytes

The maximum netty frame size in bytes. Any message received larger than this will be rejected.

**Default**: 5253120

### openFileLimit

Max number of ledger index files could be opened in bookie server If number of ledger index files reaches this limitation, bookie server started to swap some ledgers from memory to disk. Too frequent swap will affect performance. You can tune this number to gain performance according your requirements.

**Default**: 0

### pageSize

Size of a index page in ledger cache, in bytes A larger index page can improve performance writing page to disk, which is efficient when you have small number of ledgers and these ledgers have similar number of entries. If you have large number of ledgers and each ledger has fewer entries, smaller index page would improve memory usage.

**Default**: 8192

### pageLimit

How many index pages provided in ledger cache If number of index pages reaches this limitation, bookie server starts to swap some ledgers from memory to disk. You can increment this value when you found swap became more frequent. But make sure pageLimit\*pageSize should not more than JVM max memory limitation, otherwise you would got OutOfMemoryException. In general, incrementing pageLimit, using smaller index page would gain better performance in lager number of ledgers with fewer entries case If pageLimit is -1, bookie server will use 1/3 of JVM memory to compute the limitation of number of index pages.

**Default**: 0

### readOnlyModeEnabled

If all ledger directories configured are full, then support only read requests for clients. If “readOnlyModeEnabled=true” then on all ledger disks full, bookie will be converted to read-only mode and serve only read requests. Otherwise the bookie will be shutdown. By default this will be disabled.

**Default**: true

### diskUsageThreshold

For each ledger dir, maximum disk space which can be used. Default is 0.95f. i.e. 95% of disk can be used at most after which nothing will be written to that partition. If all ledger dir partitions are full, then bookie will turn to readonly mode if ‘readOnlyModeEnabled=true’ is set, else it will shutdown. Valid values should be in between 0 and 1 (exclusive).

**Default**: 0.95

### diskCheckInterval

Disk check interval in milli seconds, interval to check the ledger dirs usage.

**Default**: 10000

### auditorPeriodicCheckInterval

Interval at which the auditor will do a check of all ledgers in the cluster. By default this runs once a week. The interval is set in seconds. To disable the periodic check completely, set this to 0. Note that periodic checking will put extra load on the cluster, so it should not be run more frequently than once a day.

**Default**: 604800

### sortedLedgerStorageEnabled

Whether sorted-ledger storage is enabled.

**Default**: true

### auditorPeriodicBookieCheckInterval

The interval between auditor bookie checks. The auditor bookie check, checks ledger metadata to see which bookies should contain entries for each ledger. If a bookie which should contain entries is unavailable, thea the ledger containing that entry is marked for recovery. Setting this to 0 disabled the periodic check. Bookie checks will still run when a bookie fails. The interval is specified in seconds.

**Default**: 86400

### numAddWorkerThreads

The number of threads that should handle write requests. if zero, the writes would be handled by netty threads directly.

**Default**: 0

### numReadWorkerThreads

The number of threads that should handle read requests. if zero, the reads would be handled by netty threads directly.

**Default**: 8

### numHighPriorityWorkerThreads

The umber of threads that should be used for high priority requests (i.e. recovery reads and adds, and fencing).

**Default**: 8

### maxPendingReadRequestsPerThread

If read workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow indefinitely.

**Default**: 2500

### maxPendingAddRequestsPerThread

The limited number of pending requests, which is used to avoid the executor queue to grow indefinitely when add workers threads are enabled.

**Default**: 10000

### isForceGCAllowWhenNoSpace

Whether force compaction is allowed when the disk is full or almost full. Forcing GC could get some space back, but could also fill up the disk space more quickly. This is because new log files are created before GC, while old garbage log files are deleted after GC.

**Default**: false

### verifyMetadataOnGC

True if the bookie should double check `readMetadata` prior to GC.

**Default**: false

### flushEntrylogBytes

Entry log flush interval in bytes. Flushing in smaller chunks but more frequently reduces spikes in disk I/O. Flushing too frequently may also affect performance negatively.

**Default**: 268435456

### readBufferSizeBytes

The number of bytes we should use as capacity for BufferedReadChannel.

**Default**: 4096

### writeBufferSizeBytes

The number of bytes used as capacity for the write buffer

**Default**: 65536

### useHostNameAsBookieID

Whether the bookie should use its hostname to register with the coordination service (e.g.: zookeeper service). When false, bookie will use its ip address for the registration.

**Default**: false

### bookieId

If you want to custom a bookie ID or use a dynamic network address for the bookie, you can set the `bookieId`. <br /><br />Bookie advertises itself using the `bookieId` rather than the `BookieSocketAddress` (`hostname:port` or `IP:port`). If you set the `bookieId`, then the `useHostNameAsBookieID` does not take effect.<br /><br />The `bookieId` is a non-empty string that can contain ASCII digits and letters ([a-zA-Z9-0]), colons, dashes, and dots. <br /><br />For more information about `bookieId`, see [here](http://bookkeeper.apache.org/bps/BP-41-bookieid/).

**Default**: N/A

### allowEphemeralPorts

Whether the bookie is allowed to use an ephemeral port (port 0) as its server port. By default, an ephemeral port is not allowed. Using an ephemeral port as the service port usually indicates a configuration error. However, in unit tests, using an ephemeral port will address port conflict problems and allow running tests in parallel.

**Default**: false

### enableLocalTransport

Whether the bookie is allowed to listen for the BookKeeper clients executed on the local JVM.

**Default**: false

### disableServerSocketBind

Whether the bookie is allowed to disable bind on network interfaces. This bookie will be available only to BookKeeper clients executed on the local JVM.

**Default**: false

### skipListArenaChunkSize

The number of bytes that we should use as chunk allocation for `org.apache.bookkeeper.bookie.SkipListArena`.

**Default**: 4194304

### skipListArenaMaxAllocSize

The maximum size that we should allocate from the skiplist arena. Allocations larger than this should be allocated directly by the VM to avoid fragmentation.

**Default**: 131072

### bookieAuthProviderFactoryClass

The factory class name of the bookie authentication provider. If this is null, then there is no authentication.

**Default**: null

### statsProviderClass

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    **Default**: org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    **Default**:

### prometheusStatsHttpPort

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    **Default**: 8000

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    **Default**:

### dbStorage_writeCacheMaxSizeMb

Size of Write Cache. Memory is allocated from JVM direct memory. Write cache is used to buffer entries before flushing into the entry log. For good performance, it should be big enough to hold a substantial amount of entries in the flush interval.

**Default**: 25% of direct memory

### dbStorage_readAheadCacheMaxSizeMb

Size of Read cache. Memory is allocated from JVM direct memory. This read cache is pre-filled doing read-ahead whenever a cache miss happens. By default, it is allocated to 25% of the available direct memory.

**Default**: N/A

### dbStorage_readAheadCacheBatchSize

How many entries to pre-fill in cache after a read cache miss

**Default**: 1000

### dbStorage_rocksDB_blockCacheSize

Size of RocksDB block-cache. For best performance, this cache should be big enough to hold a significant portion of the index database which can reach ~2GB in some cases. By default, it uses 10% of direct memory.

**Default**: N/A

### dbStorage_rocksDB_writeBufferSizeMB

**Default**: 64

### dbStorage_rocksDB_sstSizeInMB

**Default**: 64

### dbStorage_rocksDB_blockSize

**Default**: 65536

### dbStorage_rocksDB_bloomFilterBitsPerKey

**Default**: 10

### dbStorage_rocksDB_numLevels

**Default**: -1

### dbStorage_rocksDB_numFilesInLevel0

**Default**: 4

### dbStorage_rocksDB_maxSizeInLevel1MB

**Default**: 256
