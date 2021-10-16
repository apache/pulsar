---
id: reference-configuration
title: Pulsar configuration
sidebar_label: Pulsar configuration
original_id: reference-configuration
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';





Pulsar configuration can be managed via a series of configuration files contained in the [`conf`](https://github.com/apache/pulsar/tree/master/conf) directory of a Pulsar [installation](getting-started-standalone)

- [BookKeeper](#bookkeeper)
- [Broker](#broker)
- [Client](#client)
- [Service discovery](#service-discovery)
- [Log4j](#log4j)
- [Log4j shell](#log4j-shell)
- [Standalone](#standalone)
- [WebSocket](#websocket)
- [Pulsar proxy](#pulsar-proxy)
- [ZooKeeper](#zookeeper)

## BookKeeper

BookKeeper is a replicated log storage system that Pulsar uses for durable storage of all messages.


|Name|Description|Default|
|---|---|---|
|bookiePort|The port on which the bookie server listens.|3181|
|allowLoopback|Whether the bookie is allowed to use a loopback interface as its primary interface (i.e. the interface used to establish its identity). By default, loopback interfaces are not allowed as the primary interface. Using a loopback interface as the primary interface usually indicates a configuration error. For example, it’s fairly common in some VPS setups to not configure a hostname or to have the hostname resolve to `127.0.0.1`. If this is the case, then all bookies in the cluster will establish their identities as `127.0.0.1:3181` and only one will be able to join the cluster. For VPSs configured like this, you should explicitly set the listening interface.|false|
|listeningInterface|The network interface on which the bookie listens. If not set, the bookie will listen on all interfaces.|eth0|
|advertisedAddress|Configure a specific hostname or IP address that the bookie should use to advertise itself to clients. If not set, bookie will advertised its own IP address or hostname, depending on the `listeningInterface` and `useHostNameAsBookieID` settings.|N/A|
|allowMultipleDirsUnderSameDiskPartition|Configure the bookie to allow/disallow multiple ledger/index/journal directories in the same filesystem disk partition|false|
|minUsableSizeForIndexFileCreation|The minimum safe usable size available in index directory for bookie to create index files while replaying journal at the time of bookie starts in Readonly Mode (in bytes).|1073741824|
|journalDirectory|The directory where Bookkeeper outputs its write-ahead log (WAL)|data/bookkeeper/journal|
|journalDirectories|Directories that BookKeeper outputs its write ahead log. Multi directories are available, being separated by `,`. For example: `journalDirectories=/tmp/bk-journal1,/tmp/bk-journal2`. If `journalDirectories` is set, bookies will skip `journalDirectory` and use this setting directory.|/tmp/bk-journal|
|ledgerDirectories|The directory where Bookkeeper outputs ledger snapshots. This could define multiple directories to store snapshots separated by comma, for example `ledgerDirectories=/tmp/bk1-data,/tmp/bk2-data`. Ideally, ledger dirs and the journal dir are each in a different device, which reduces the contention between random I/O and sequential write. It is possible to run with a single disk, but performance will be significantly lower.|data/bookkeeper/ledgers|
|ledgerManagerType|The type of ledger manager used to manage how ledgers are stored, managed, and garbage collected. See [BookKeeper Internals](http://bookkeeper.apache.org/docs/latest/getting-started/concepts) for more info.|hierarchical|
|zkLedgersRootPath|The root ZooKeeper path used to store ledger metadata. This parameter is used by the ZooKeeper-based ledger manager as a root znode to store all ledgers.|/ledgers|
|ledgerStorageClass|Ledger storage implementation class|org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage|
|entryLogFilePreallocationEnabled|Enable or disable entry logger preallocation|true|
|logSizeLimit|Max file size of the entry logger, in bytes. A new entry log file will be created when the old one reaches the file size limitation.|2147483648|
|minorCompactionThreshold|Threshold of minor compaction. Entry log files whose remaining size percentage reaches below this threshold will be compacted in a minor compaction. If set to less than zero, the minor compaction is disabled.|0.2|
|minorCompactionInterval|Time interval to run minor compaction, in seconds. If set to less than zero, the minor compaction is disabled. Note: should be greater than gcWaitTime. |3600|
|majorCompactionThreshold|The threshold of major compaction. Entry log files whose remaining size percentage reaches below this threshold will be compacted in a major compaction. Those entry log files whose remaining size percentage is still higher than the threshold will never be compacted. If set to less than zero, the minor compaction is disabled.|0.5|
|majorCompactionInterval|The time interval to run major compaction, in seconds. If set to less than zero, the major compaction is disabled. Note: should be greater than gcWaitTime. |86400|
|readOnlyModeEnabled|If `readOnlyModeEnabled=true`, then on all full ledger disks, bookie will be converted to read-only mode and serve only read requests. Otherwise the bookie will be shutdown.|true|
|forceReadOnlyBookie|Whether the bookie is force started in read only mode.|false|
|persistBookieStatusEnabled|Persist the bookie status locally on the disks. So the bookies can keep their status upon restarts.|false|
|compactionMaxOutstandingRequests|Sets the maximum number of entries that can be compacted without flushing. When compacting, the entries are written to the entrylog and the new offsets are cached in memory. Once the entrylog is flushed the index is updated with the new offsets. This parameter controls the number of entries added to the entrylog before a flush is forced. A higher value for this parameter means more memory will be used for offsets. Each offset consists of 3 longs. This parameter should not be modified unless you’re fully aware of the consequences.|100000|
|compactionRate|The rate at which compaction will read entries, in adds per second.|1000|
|isThrottleByBytes|Throttle compaction by bytes or by entries.|false|
|compactionRateByEntries|The rate at which compaction will read entries, in adds per second.|1000|
|compactionRateByBytes|Set the rate at which compaction reads entries. The unit is bytes added per second.|1000000|
|journalMaxSizeMB|Max file size of journal file, in megabytes. A new journal file will be created when the old one reaches the file size limitation.|2048|
|journalMaxBackups|The max number of old journal files to keep. Keeping a number of old journal files would help data recovery in special cases.|5|
|journalPreAllocSizeMB|How space to pre-allocate at a time in the journal.|16|
|journalWriteBufferSizeKB|The of the write buffers used for the journal.|64|
|journalRemoveFromPageCache|Whether pages should be removed from the page cache after force write.|true|
|journalAdaptiveGroupWrites|Whether to group journal force writes, which optimizes group commit for higher throughput.|true|
|journalMaxGroupWaitMSec|The maximum latency to impose on a journal write to achieve grouping.|1|
|journalAlignmentSize|All the journal writes and commits should be aligned to given size|4096|
|journalBufferedWritesThreshold|Maximum writes to buffer to achieve grouping|524288|
|journalFlushWhenQueueEmpty|If we should flush the journal when journal queue is empty|false|
|numJournalCallbackThreads|The number of threads that should handle journal callbacks|8|
|openLedgerRereplicationGracePeriod | The grace period, in milliseconds, that the replication worker waits before fencing and replicating a ledger fragment that's still being written to upon bookie failure. | 30000 |
|rereplicationEntryBatchSize|The number of max entries to keep in fragment for re-replication|100|
|autoRecoveryDaemonEnabled|Whether the bookie itself can start auto-recovery service.|true|
|lostBookieRecoveryDelay|How long to wait, in seconds, before starting auto recovery of a lost bookie.|0|
|gcWaitTime|How long the interval to trigger next garbage collection, in milliseconds. Since garbage collection is running in background, too frequent gc will heart performance. It is better to give a higher number of gc interval if there is enough disk capacity.|900000|
|gcOverreplicatedLedgerWaitTime|How long the interval to trigger next garbage collection of overreplicated ledgers, in milliseconds. This should not be run very frequently since we read the metadata for all the ledgers on the bookie from zk.|86400000|
|flushInterval|How long the interval to flush ledger index pages to disk, in milliseconds. Flushing index files will introduce much random disk I/O. If separating journal dir and ledger dirs each on different devices, flushing would not affect performance. But if putting journal dir and ledger dirs on same device, performance degrade significantly on too frequent flushing. You can consider increment flush interval to get better performance, but you need to pay more time on bookie server restart after failure.|60000|
|bookieDeathWatchInterval|Interval to watch whether bookie is dead or not, in milliseconds|1000|
|allowStorageExpansion|Allow the bookie storage to expand. Newly added ledger and index dirs must be empty.|false|
|zkServers|A list of one of more servers on which zookeeper is running. The server list can be comma separated values, for example: zkServers=zk1:2181,zk2:2181,zk3:2181.|localhost:2181|
|zkTimeout|ZooKeeper client session timeout in milliseconds Bookie server will exit if it received SESSION_EXPIRED because it was partitioned off from ZooKeeper for more than the session timeout JVM garbage collection, disk I/O will cause SESSION_EXPIRED. Increment this value could help avoiding this issue|30000|
|zkRetryBackoffStartMs|The start time that the Zookeeper client backoff retries in milliseconds.|1000|
|zkRetryBackoffMaxMs|The maximum time that the Zookeeper client backoff retries in milliseconds.|10000|
|zkEnableSecurity|Set ACLs on every node written on ZooKeeper, allowing users to read and write BookKeeper metadata stored on ZooKeeper. In order to make ACLs work you need to setup ZooKeeper JAAS authentication. All the bookies and Client need to share the same user, and this is usually done using Kerberos authentication. See ZooKeeper documentation.|false|
|httpServerEnabled|The flag enables/disables starting the admin http server.|false|
|httpServerPort|The HTTP server port to listen on. By default, the value is `8080`. If you want to keep it consistent with the Prometheus stats provider, you can set it to `8000`.|8080
|httpServerClass|The http server class.|org.apache.bookkeeper.http.vertx.VertxHttpServer|
|serverTcpNoDelay|This settings is used to enabled/disabled Nagle’s algorithm, which is a means of improving the efficiency of TCP/IP networks by reducing the number of packets that need to be sent over the network. If you are sending many small messages, such that more than one can fit in a single IP packet, setting server.tcpnodelay to false to enable Nagle algorithm can provide better performance.|true|
|serverSockKeepalive|This setting is used to send keep-alive messages on connection-oriented sockets.|true|
|serverTcpLinger|The socket linger timeout on close. When enabled, a close or shutdown will not return until all queued messages for the socket have been successfully sent or the linger timeout has been reached. Otherwise, the call returns immediately and the closing is done in the background.|0|
|byteBufAllocatorSizeMax|The maximum buf size of the received ByteBuf allocator.|1048576|
|nettyMaxFrameSizeBytes|The maximum netty frame size in bytes. Any message received larger than this will be rejected.|5253120|
|openFileLimit|Max number of ledger index files could be opened in bookie server If number of ledger index files reaches this limitation, bookie server started to swap some ledgers from memory to disk. Too frequent swap will affect performance. You can tune this number to gain performance according your requirements.|0|
|pageSize|Size of a index page in ledger cache, in bytes A larger index page can improve performance writing page to disk, which is efficient when you have small number of ledgers and these ledgers have similar number of entries. If you have large number of ledgers and each ledger has fewer entries, smaller index page would improve memory usage.|8192|
|pageLimit|How many index pages provided in ledger cache If number of index pages reaches this limitation, bookie server starts to swap some ledgers from memory to disk. You can increment this value when you found swap became more frequent. But make sure pageLimit*pageSize should not more than JVM max memory limitation, otherwise you would got OutOfMemoryException. In general, incrementing pageLimit, using smaller index page would gain better performance in lager number of ledgers with fewer entries case If pageLimit is -1, bookie server will use 1/3 of JVM memory to compute the limitation of number of index pages.|0|
|readOnlyModeEnabled|If all ledger directories configured are full, then support only read requests for clients. If “readOnlyModeEnabled=true” then on all ledger disks full, bookie will be converted to read-only mode and serve only read requests. Otherwise the bookie will be shutdown. By default this will be disabled.|true|
|diskUsageThreshold|For each ledger dir, maximum disk space which can be used. Default is 0.95f. i.e. 95% of disk can be used at most after which nothing will be written to that partition. If all ledger dir partitions are full, then bookie will turn to readonly mode if ‘readOnlyModeEnabled=true’ is set, else it will shutdown. Valid values should be in between 0 and 1 (exclusive).|0.95|
|diskCheckInterval|Disk check interval in milli seconds, interval to check the ledger dirs usage.|10000|
|auditorPeriodicCheckInterval|Interval at which the auditor will do a check of all ledgers in the cluster. By default this runs once a week. The interval is set in seconds. To disable the periodic check completely, set this to 0. Note that periodic checking will put extra load on the cluster, so it should not be run more frequently than once a day.|604800|
|sortedLedgerStorageEnabled|Whether sorted-ledger storage is enabled.|true|
|auditorPeriodicBookieCheckInterval|The interval between auditor bookie checks. The auditor bookie check, checks ledger metadata to see which bookies should contain entries for each ledger. If a bookie which should contain entries is unavailable, thea the ledger containing that entry is marked for recovery. Setting this to 0 disabled the periodic check. Bookie checks will still run when a bookie fails. The interval is specified in seconds.|86400|
|numAddWorkerThreads|The number of threads that should handle write requests. if zero, the writes would be handled by netty threads directly.|0|
|numReadWorkerThreads|The number of threads that should handle read requests. if zero, the reads would be handled by netty threads directly.|8|
|numHighPriorityWorkerThreads|The umber of threads that should be used for high priority requests (i.e. recovery reads and adds, and fencing).|8|
|maxPendingReadRequestsPerThread|If read workers threads are enabled, limit the number of pending requests, to avoid the executor queue to grow indefinitely.|2500|
|maxPendingAddRequestsPerThread|The limited number of pending requests, which is used to avoid the executor queue to grow indefinitely when add workers threads are enabled.|10000|
|isForceGCAllowWhenNoSpace|Whether force compaction is allowed when the disk is full or almost full. Forcing GC could get some space back, but could also fill up the disk space more quickly. This is because new log files are created before GC, while old garbage log files are deleted after GC.|false|
|verifyMetadataOnGC|True if the bookie should double check `readMetadata` prior to GC.|false|
|flushEntrylogBytes|Entry log flush interval in bytes. Flushing in smaller chunks but more frequently reduces spikes in disk I/O. Flushing too frequently may also affect performance negatively.|268435456|
|readBufferSizeBytes|The number of bytes we should use as capacity for BufferedReadChannel.|4096|
|writeBufferSizeBytes|The number of bytes used as capacity for the write buffer|65536|
|useHostNameAsBookieID|Whether the bookie should use its hostname to register with the coordination service (e.g.: zookeeper service). When false, bookie will use its ip address for the registration.|false|
|bookieId | If you want to custom a bookie ID or use a dynamic network address for the bookie, you can set the `bookieId`. <br /><br />Bookie advertises itself using the `bookieId` rather than the `BookieSocketAddress` (`hostname:port` or `IP:port`). If you set the `bookieId`, then the `useHostNameAsBookieID` does not take effect.<br /><br />The `bookieId` is a non-empty string that can contain ASCII digits and letters ([a-zA-Z9-0]), colons, dashes, and dots. <br /><br />For more information about `bookieId`, see [here](http://bookkeeper.apache.org/bps/BP-41-bookieid/).|N/A|
|allowEphemeralPorts|Whether the bookie is allowed to use an ephemeral port (port 0) as its server port. By default, an ephemeral port is not allowed. Using an ephemeral port as the service port usually indicates a configuration error. However, in unit tests, using an ephemeral port will address port conflict problems and allow running tests in parallel.|false|
|enableLocalTransport|Whether the bookie is allowed to listen for the BookKeeper clients executed on the local JVM.|false|
|disableServerSocketBind|Whether the bookie is allowed to disable bind on network interfaces. This bookie will be available only to BookKeeper clients executed on the local JVM.|false|
|skipListArenaChunkSize|The number of bytes that we should use as chunk allocation for `org.apache.bookkeeper.bookie.SkipListArena`.|4194304|
|skipListArenaMaxAllocSize|The maximum size that we should allocate from the skiplist arena. Allocations larger than this should be allocated directly by the VM to avoid fragmentation.|131072|
|bookieAuthProviderFactoryClass|The factory class name of the bookie authentication provider. If this is null, then there is no authentication.|null|
|statsProviderClass||org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider|
|prometheusStatsHttpPort||8000|
|dbStorage_writeCacheMaxSizeMb|Size of Write Cache. Memory is allocated from JVM direct memory. Write cache is used to buffer entries before flushing into the entry log. For good performance, it should be big enough to hold a substantial amount of entries in the flush interval.|25% of direct memory|
|dbStorage_readAheadCacheMaxSizeMb|Size of Read cache. Memory is allocated from JVM direct memory. This read cache is pre-filled doing read-ahead whenever a cache miss happens. By default, it is allocated to 25% of the available direct memory.|N/A|
|dbStorage_readAheadCacheBatchSize|How many entries to pre-fill in cache after a read cache miss|1000|
|dbStorage_rocksDB_blockCacheSize|Size of RocksDB block-cache. For best performance, this cache should be big enough to hold a significant portion of the index database which can reach ~2GB in some cases. By default, it uses 10% of direct memory.|N/A|
|dbStorage_rocksDB_writeBufferSizeMB||64|
|dbStorage_rocksDB_sstSizeInMB||64|
|dbStorage_rocksDB_blockSize||65536|
|dbStorage_rocksDB_bloomFilterBitsPerKey||10|
|dbStorage_rocksDB_numLevels||-1|
|dbStorage_rocksDB_numFilesInLevel0||4|
|dbStorage_rocksDB_maxSizeInLevel1MB||256|


## Broker

Pulsar brokers are responsible for handling incoming messages from producers, dispatching messages to consumers, replicating data between clusters, and more.

|Name|Description|Default|
|---|---|---|
|advertisedListeners|Specify multiple advertised listeners for the broker.<br /><br />The format is `<listener_name>:pulsar://<host>:<port>`.<br /><br />If there are multiple listeners, separate them with commas.<br /><br />**Note**: do not use this configuration with `advertisedAddress` and `brokerServicePort`. If the value of this configuration is empty, the broker uses `advertisedAddress` and `brokerServicePort`|/|
|internalListenerName|Specify the internal listener name for the broker.<br /><br />**Note**: the listener name must be contained in `advertisedListeners`.<br /><br /> If the value of this configuration is empty, the broker uses the first listener as the internal listener.|/|
|authenticateOriginalAuthData|  If this flag is set to `true`, the broker authenticates the original Auth data; else it just accepts the originalPrincipal and authorizes it (if required). |false|
|enablePersistentTopics|  Whether persistent topics are enabled on the broker |true|
|enableNonPersistentTopics| Whether non-persistent topics are enabled on the broker |true|
|functionsWorkerEnabled|  Whether the Pulsar Functions worker service is enabled in the broker  |false|
|exposePublisherStats|Whether to enable topic level metrics.|true|
|statsUpdateFrequencyInSecs||60|
|statsUpdateInitialDelayInSecs||60|
|zookeeperServers|  Zookeeper quorum connection string  ||
|zooKeeperCacheExpirySeconds|ZooKeeper cache expiry time in seconds|300|
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
|brokerServicePort| Broker data port  |6650|
|brokerServicePortTls|  Broker data port for TLS  |6651|
|webServicePort|  Port to use to server HTTP request  |8080|
|webServicePortTls| Port to use to server HTTPS request |8443|
|webSocketServiceEnabled| Enable the WebSocket API service in broker  |false|
|webSocketNumIoThreads|The number of IO threads in Pulsar Client used in WebSocket proxy.|8|
|webSocketConnectionsPerBroker|The number of connections per Broker in Pulsar Client used in WebSocket proxy.|8|
|webSocketSessionIdleTimeoutMillis|Time in milliseconds that idle WebSocket session times out.|300000|
|webSocketMaxTextFrameSize|The maximum size of a text message during parsing in WebSocket proxy.|1048576|
|exposeTopicLevelMetricsInPrometheus|Whether to enable topic level metrics.|true|
|exposeConsumerLevelMetricsInPrometheus|Whether to enable consumer level metrics.|false|
|jvmGCMetricsLoggerClassName|Classname of Pluggable JVM GC metrics logger that can log GC specific metrics.|N/A|
|bindAddress| Hostname or IP address the service binds on, default is 0.0.0.0.  |0.0.0.0|
|advertisedAddress| Hostname or IP address the service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getHostName()` is used.  ||
|clusterName| Name of the cluster to which this broker belongs to ||
|brokerDeduplicationEnabled|  Sets the default behavior for message deduplication in the broker. If enabled, the broker will reject messages that were already stored in the topic. This setting can be overridden on a per-namespace basis.  |false|
|brokerDeduplicationMaxNumberOfProducers| The maximum number of producers for which information will be stored for deduplication purposes.  |10000|
|brokerDeduplicationEntriesInterval|  The number of entries after which a deduplication informational snapshot is taken. A larger interval will lead to fewer snapshots being taken, though this would also lengthen the topic recovery time (the time required for entries published after the snapshot to be replayed). |1000|
|brokerDeduplicationProducerInactivityTimeoutMinutes| The time of inactivity (in minutes) after which the broker will discard deduplication information related to a disconnected producer. |360|
|dispatchThrottlingRatePerReplicatorInMsg| The default messages per second dispatch throttling-limit for every replicator in replication. The value of `0` means disabling replication message dispatch-throttling| 0 |
|dispatchThrottlingRatePerReplicatorInByte| The default bytes per second dispatch throttling-limit for every replicator in replication. The value of `0` means disabling replication message-byte dispatch-throttling| 0 | 
|zooKeeperSessionTimeoutMillis| Zookeeper session timeout in milliseconds |30000|
|brokerShutdownTimeoutMs| Time to wait for broker graceful shutdown. After this time elapses, the process will be killed  |60000|
|skipBrokerShutdownOnOOM| Flag to skip broker shutdown when broker handles Out of memory error. |false|
|backlogQuotaCheckEnabled|  Enable backlog quota check. Enforces action on topic when the quota is reached  |true|
|backlogQuotaCheckIntervalInSeconds|  How often to check for topics that have reached the quota |60|
|backlogQuotaDefaultLimitGB| The default per-topic backlog quota limit. Being less than 0 means no limitation. By default, it is -1. | -1 |
|backlogQuotaDefaultRetentionPolicy|The defaulted backlog quota retention policy. By Default, it is `producer_request_hold`. <li>'producer_request_hold' Policy which holds producer's send request until the resource becomes available (or holding times out)</li> <li>'producer_exception' Policy which throws `javax.jms.ResourceAllocationException` to the producer </li><li>'consumer_backlog_eviction' Policy which evicts the oldest message from the slowest consumer's backlog</li>|producer_request_hold|
|allowAutoTopicCreation| Enable topic auto creation if a new producer or consumer connected |true|
|allowAutoTopicCreationType| The type of topic that is allowed to be automatically created.(partitioned/non-partitioned) |non-partitioned|
|allowAutoSubscriptionCreation| Enable subscription auto creation if a new consumer connected |true|
|defaultNumPartitions| The number of partitioned topics that is allowed to be automatically created if `allowAutoTopicCreationType` is partitioned |1|
|brokerDeleteInactiveTopicsEnabled| Enable the deletion of inactive topics. If topics are not consumed for some while, these inactive topics might be cleaned up. Deleting inactive topics is enabled by default. The default period is 1 minute.  |true|
|brokerDeleteInactiveTopicsFrequencySeconds|  How often to check for inactive topics  |60|
| brokerDeleteInactiveTopicsMode | Set the mode to delete inactive topics. <li> `delete_when_no_subscriptions`: delete the topic which has no subscriptions or active producers. </li><li> `delete_when_subscriptions_caught_up`: delete the topic whose subscriptions have no backlogs and which has no active producers or consumers. </li>| `delete_when_no_subscriptions` |
| brokerDeleteInactiveTopicsMaxInactiveDurationSeconds | Set the maximum duration for inactive topics. If it is not specified, the `brokerDeleteInactiveTopicsFrequencySeconds` parameter is adopted. | N/A |
|messageExpiryCheckIntervalInMinutes| How frequently to proactively check and purge expired messages  |5|
|brokerServiceCompactionMonitorIntervalInSeconds| Interval between checks to see if topics with compaction policies need to be compacted  |60|
|delayedDeliveryEnabled|Whether to enable the delayed delivery for messages. If disabled, messages will be immediately delivered and there will be no tracking overhead.|true|
|delayedDeliveryTickTimeMillis|Control the tick time for retrying on delayed delivery, which affecte the accuracy of the delivery time compared to the scheduled time. By default, it is 1 second.|1000|
|activeConsumerFailoverDelayTimeMillis| How long to delay rewinding cursor and dispatching messages when active consumer is changed.  |1000|
|clientLibraryVersionCheckEnabled|  Enable check for minimum allowed client library version |false|
|clientLibraryVersionCheckAllowUnversioned| Allow client libraries with no version information  |true|
|statusFilePath|  Path for the file used to determine the rotation status for the broker when responding to service discovery health checks ||
|preferLaterVersions| If true, (and ModularLoadManagerImpl is being used), the load manager will attempt to use only brokers running the latest software version (to minimize impact to bundles)  |false|
|maxNumPartitionsPerPartitionedTopic|Max number of partitions per partitioned topic. Use 0 or negative number to disable the check|0|
|tlsEnabled|Deprecated - Use `webServicePortTls` and `brokerServicePortTls` instead. |false|
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||
|tlsTrustCertsFilePath| Path for the trusted TLS certificate file. This cert is used to verify that any certs presented by connecting clients are signed by a certificate authority. If this verification fails, then the certs are untrusted and the connections are dropped. ||
|tlsAllowInsecureConnection| Accept untrusted TLS certificate from client. If it is set to `true`, a client with a cert which cannot be verified with the 'tlsTrustCertsFilePath' cert will be allowed to connect to the server, though the cert will not be used for client authentication. |false|
|tlsProtocols|Specify the tls protocols the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLSv1.2```, ```TLSv1.1```, ```TLSv1``` ||
|tlsCiphers|Specify the tls cipher the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256```||
|tlsEnabledWithKeyStore| Enable TLS with KeyStore type configuration in broker |false|
|tlsProvider| TLS Provider for KeyStore type ||
|tlsKeyStoreType| LS KeyStore type configuration in broker: JKS, PKCS12 |JKS|
|tlsKeyStore| TLS KeyStore path in broker ||
|tlsKeyStorePassword| TLS KeyStore password for broker ||
|brokerClientTlsEnabledWithKeyStore| Whether internal client use KeyStore type to authenticate with Pulsar brokers |false|
|brokerClientSslProvider| The TLS Provider used by internal client to authenticate with other Pulsar brokers ||
|brokerClientTlsTrustStoreType| TLS TrustStore type configuration for internal client: JKS, PKCS12, used by the internal client to authenticate with Pulsar brokers |JKS|
|brokerClientTlsTrustStore| TLS TrustStore path for internal client, used by the internal client to authenticate with Pulsar brokers ||
|brokerClientTlsTrustStorePassword| TLS TrustStore password for internal client, used by the internal client to authenticate with Pulsar brokers ||
|brokerClientTlsCiphers| Specify the tls cipher the internal client will use to negotiate during TLS Handshake. (a comma-separated list of ciphers) e.g.  [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]||
|brokerClientTlsProtocols|Specify the tls protocols the broker will use to negotiate during TLS handshake. (a comma-separated list of protocol names). e.g.  [TLSv1.2, TLSv1.1, TLSv1] ||
|ttlDurationDefaultInSeconds|  The default Time to Live (TTL) for namespaces if the TTL is not configured at namespace policies. When the value is set to `0`, TTL is disabled. By default, TTL is disabled. |0|
|tokenSecretKey| Configure the secret key to be used to validate auth tokens. The key can be specified like: `tokenSecretKey=data:;base64,xxxxxxxxx` or `tokenSecretKey=file:///my/secret.key`||
|tokenPublicKey| Configure the public key to be used to validate auth tokens. The key can be specified like: `tokenPublicKey=data:;base64,xxxxxxxxx` or `tokenPublicKey=file:///my/secret.key`||
|tokenPublicAlg| Configure the algorithm to be used to validate auth tokens. This can be any of the asymettric algorithms supported by Java JWT (https://github.com/jwtk/jjwt#signature-algorithms-keys) |RS256|
|tokenAuthClaim| Specify which of the token's claims will be used as the authentication "principal" or "role". The default "sub" claim will be used if this is left blank ||
|tokenAudienceClaim| The token audience "claim" name, e.g. "aud", that will be used to get the audience from token. If not set, audience will not be verified. ||
|tokenAudience| The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token, need contains this. ||
|maxUnackedMessagesPerConsumer| Max number of unacknowledged messages allowed to receive messages by a consumer on a shared subscription. Broker will stop sending messages to consumer once, this limit reaches until consumer starts acknowledging messages back. Using a value of 0, is disabling unackeMessage limit check and consumer can receive messages without any restriction  |50000|
|maxUnackedMessagesPerSubscription| Max number of unacknowledged messages allowed per shared subscription. Broker will stop dispatching messages to all consumers of the subscription once this limit reaches until consumer starts acknowledging messages back and unack count reaches to limit/2. Using a value of 0, is disabling unackedMessage-limit check and dispatcher can dispatch messages without any restriction  |200000|
|subscriptionRedeliveryTrackerEnabled| Enable subscription message redelivery tracker |true|
|subscriptionExpirationTimeMinutes | How long to delete inactive subscriptions from last consuming. <br /><br />Setting this configuration to a value **greater than 0** deletes inactive subscriptions automatically.<br />Setting this configuration to **0** does not delete inactive subscriptions automatically. <br /><br /> Since this configuration takes effect on all topics, if there is even one topic whose subscriptions should not be deleted automatically, you need to set it to 0. <br />Instead, you can set a subscription expiration time for each **namespace** using the [`pulsar-admin namespaces set-subscription-expiration-time options` command](https://pulsar.apache.org/tools/pulsar-admin/2.6.0-SNAPSHOT/#-em-set-subscription-expiration-time-em-). | 0 |
|maxConcurrentLookupRequest|  Max number of concurrent lookup request broker allows to throttle heavy incoming lookup traffic |50000|
|maxConcurrentTopicLoadRequest| Max number of concurrent topic loading request broker allows to control number of zk-operations |5000|
|authenticationEnabled| Enable authentication |false|
|authenticationProviders| Authentication provider name list, which is comma separated list of class names  ||
| authenticationRefreshCheckSeconds | Interval of time for checking for expired authentication credentials | 60 |
|authorizationEnabled|  Enforce authorization |false|
|superUserRoles|  Role names that are treated as “super-user”, meaning they will be able to do all admin operations and publish/consume from all topics ||
|brokerClientAuthenticationPlugin|  Authentication settings of the broker itself. Used when the broker connects to other brokers, either in same or other clusters  ||
|brokerClientAuthenticationParameters|||
|athenzDomainNames| Supported Athenz provider domain names(comma separated) for authentication  ||
|exposePreciseBacklogInPrometheus| Enable expose the precise backlog stats, set false to use published counter and consumed counter to calculate, this would be more efficient but may be inaccurate. |false|
|schemaRegistryStorageClassName|The schema storage implementation used by this broker.|org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory|
|isSchemaValidationEnforced|Enforce schema validation on following cases: if a producer without a schema attempts to produce to a topic with schema, the producer will be failed to connect. PLEASE be carefully on using this, since non-java clients don't support schema. If this setting is enabled, then non-java clients fail to produce.|false|
|offloadersDirectory|The directory for all the offloader implementations.|./offloaders|
|bookkeeperMetadataServiceUri| Metadata service uri that bookkeeper is used for loading corresponding metadata driver and resolving its metadata service location. This value can be fetched using `bookkeeper shell whatisinstanceid` command in BookKeeper cluster. For example: zk+hierarchical://localhost:2181/ledgers. The metadata service uri list can also be semicolon separated values like below: zk+hierarchical://zk1:2181;zk2:2181;zk3:2181/ledgers ||
|bookkeeperClientAuthenticationPlugin|  Authentication plugin to use when connecting to bookies ||
|bookkeeperClientAuthenticationParametersName|  BookKeeper auth plugin implementation specifics parameters name and values  ||
|bookkeeperClientAuthenticationParameters|||
|bookkeeperClientTimeoutInSeconds|  Timeout for BK add / read operations  |30|
|bookkeeperClientSpeculativeReadTimeoutInMillis|  Speculative reads are initiated if a read request doesn’t complete within a certain time Using a value of 0, is disabling the speculative reads |0|
|bookkeeperNumberOfChannelsPerBookie|  Number of channels per bookie  |16|
|bookkeeperClientHealthCheckEnabled|  Enable bookies health check. Bookies that have more than the configured number of failure within the interval will be quarantined for some time. During this period, new ledgers won’t be created on these bookies  |true|
|bookkeeperClientHealthCheckIntervalSeconds||60|
|bookkeeperClientHealthCheckErrorThresholdPerInterval||5|
|bookkeeperClientHealthCheckQuarantineTimeInSeconds ||1800|
|bookkeeperClientRackawarePolicyEnabled|  Enable rack-aware bookie selection policy. BK will chose bookies from different racks when forming a new bookie ensemble  |true|
|bookkeeperClientRegionawarePolicyEnabled|  Enable region-aware bookie selection policy. BK will chose bookies from different regions and racks when forming a new bookie ensemble. If enabled, the value of bookkeeperClientRackawarePolicyEnabled is ignored  |false|
|bookkeeperClientMinNumRacksPerWriteQuorum|  Minimum number of racks per write quorum. BK rack-aware bookie selection policy will try to get bookies from at least 'bookkeeperClientMinNumRacksPerWriteQuorum' racks for a write quorum.  |2|
|bookkeeperClientEnforceMinNumRacksPerWriteQuorum|  Enforces rack-aware bookie selection policy to pick bookies from 'bookkeeperClientMinNumRacksPerWriteQuorum' racks for a writeQuorum. If BK can't find bookie then it would throw BKNotEnoughBookiesException instead of picking random one.  |false|
|bookkeeperClientReorderReadSequenceEnabled|  Enable/disable reordering read sequence on reading entries.  |false|
|bookkeeperClientIsolationGroups| Enable bookie isolation by specifying a list of bookie groups to choose from. Any bookie outside the specified groups will not be used by the broker  ||
|bookkeeperClientSecondaryIsolationGroups| Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough bookie available.  ||
|bookkeeperClientMinAvailableBookiesInIsolationGroups| Minimum bookies that should be available as part of bookkeeperClientIsolationGroups else broker will include bookkeeperClientSecondaryIsolationGroups bookies in isolated list.  ||
|bookkeeperClientGetBookieInfoIntervalSeconds| Set the interval to periodically check bookie info |86400|
|bookkeeperClientGetBookieInfoRetryIntervalSeconds|  Set the interval to retry a failed bookie info lookup |60|
|bookkeeperEnableStickyReads | Enable/disable having read operations for a ledger to be sticky to a single bookie. If this flag is enabled, the client will use one single bookie (by preference) to read  all entries for a ledger. | true |
|managedLedgerDefaultEnsembleSize|  Number of bookies to use when creating a ledger |2|
|managedLedgerDefaultWriteQuorum| Number of copies to store for each message  |2|
|managedLedgerDefaultAckQuorum| Number of guaranteed copies (acks to wait before write is complete) |2|
|managedLedgerCacheSizeMB|  Amount of memory to use for caching data payload in managed ledger. This memory is allocated from JVM direct memory and it’s shared across all the topics running in the same broker. By default, uses 1/5th of available direct memory ||
|managedLedgerCacheCopyEntries| Whether we should make a copy of the entry payloads when inserting in cache| false|
|managedLedgerCacheEvictionWatermark| Threshold to which bring down the cache level when eviction is triggered  |0.9|
|managedLedgerCacheEvictionFrequency| Configure the cache eviction frequency for the managed ledger cache (evictions/sec) | 100.0 |
|managedLedgerCacheEvictionTimeThresholdMillis| All entries that have stayed in cache for more than the configured time, will be evicted | 1000 |
|managedLedgerCursorBackloggedThreshold| Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged' and thus should be set as inactive. | 1000|
|managedLedgerDefaultMarkDeleteRateLimit| Rate limit the amount of writes per second generated by consumer acking the messages  |1.0|
|managedLedgerMaxEntriesPerLedger| The max number of entries to append to a ledger before triggering a rollover. A ledger rollover is triggered after the min rollover time has passed and one of the following conditions is true: <ul><li>The max rollover time has been reached</li><li>The max entries have been written to the ledger</li><li>The max ledger size has been written to the ledger</li></ul>|50000|
|managedLedgerMinLedgerRolloverTimeMinutes| Minimum time between ledger rollover for a topic  |10|
|managedLedgerMaxLedgerRolloverTimeMinutes| Maximum time before forcing a ledger rollover for a topic |240|
|managedLedgerCursorMaxEntriesPerLedger|  Max number of entries to append to a cursor ledger  |50000|
|managedLedgerCursorRolloverTimeInSeconds|  Max time before triggering a rollover on a cursor ledger  |14400|
|managedLedgerMaxUnackedRangesToPersist|  Max number of “acknowledgment holes” that are going to be persistently stored. When acknowledging out of order, a consumer will leave holes that are supposed to be quickly filled by acking all the messages. The information of which messages are acknowledged is persisted by compressing in “ranges” of messages that were acknowledged. After the max number of ranges is reached, the information will only be tracked in memory and messages will be redelivered in case of crashes.  |1000|
|autoSkipNonRecoverableData|  Skip reading non-recoverable/unreadable data-ledger under managed-ledger’s list.It helps when data-ledgers gets corrupted at bookkeeper and managed-cursor is stuck at that ledger. |false|
|loadBalancerEnabled| Enable load balancer  |true|
|loadBalancerPlacementStrategy| Strategy to assign a new bundle weightedRandomSelection ||
|loadBalancerReportUpdateThresholdPercentage| Percentage of change to trigger load report update  |10|
|loadBalancerReportUpdateMaxIntervalMinutes|  maximum interval to update load report  |15|
|loadBalancerHostUsageCheckIntervalMinutes| Frequency of report to collect  |1|
|loadBalancerSheddingIntervalMinutes| Load shedding interval. Broker periodically checks whether some traffic should be offload from some over-loaded broker to other under-loaded brokers  |30|
|loadBalancerSheddingGracePeriodMinutes|  Prevent the same topics to be shed and moved to other broker more than once within this timeframe |30|
|loadBalancerBrokerMaxTopics| Usage threshold to allocate max number of topics to broker  |50000|
|loadBalancerBrokerUnderloadedThresholdPercentage|  Usage threshold to determine a broker as under-loaded |1|
|loadBalancerBrokerOverloadedThresholdPercentage| Usage threshold to determine a broker as over-loaded  |85|
|loadBalancerResourceQuotaUpdateIntervalMinutes|  Interval to update namespace bundle resource quota |15|
|loadBalancerBrokerComfortLoadLevelPercentage|  Usage threshold to determine a broker is having just right level of load  |65|
|loadBalancerAutoBundleSplitEnabled|  enable/disable namespace bundle auto split  |false|
|loadBalancerNamespaceBundleMaxTopics|  maximum topics in a bundle, otherwise bundle split will be triggered  |1000|
|loadBalancerNamespaceBundleMaxSessions|  maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered  |1000|
|loadBalancerNamespaceBundleMaxMsgRate| maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered  |1000|
|loadBalancerNamespaceBundleMaxBandwidthMbytes| maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered  |100|
|loadBalancerNamespaceMaximumBundles| maximum number of bundles in a namespace  |128|
|replicationMetricsEnabled| Enable replication metrics  |true|
|replicationConnectionsPerBroker| Max number of connections to open for each broker in a remote cluster More connections host-to-host lead to better throughput over high-latency links.  |16|
|replicationProducerQueueSize|  Replicator producer queue size  |1000|
|replicatorPrefix|  Replicator prefix used for replicator producer name and cursor name pulsar.repl||
|replicationTlsEnabled| Enable TLS when talking with other clusters to replicate messages |false|
|brokerServicePurgeInactiveFrequencyInSeconds|Deprecated. Use `brokerDeleteInactiveTopicsFrequencySeconds`.|60|
|transactionCoordinatorEnabled|Whether to enable transaction coordinator in broker.|true|
|transactionMetadataStoreProviderClassName| |org.apache.pulsar.transaction.coordinator.impl.InMemTransactionMetadataStoreProvider|
|defaultRetentionTimeInMinutes| Default message retention time  |0|
|defaultRetentionSizeInMB|  Default retention size  |0|
|keepAliveIntervalSeconds|  How often to check whether the connections are still alive  |30|
|bootstrapNamespaces| The bootstrap name. | N/A |
|loadManagerClassName|  Name of load manager to use |org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl|
|supportedNamespaceBundleSplitAlgorithms| Supported algorithms name for namespace bundle split |[range_equally_divide,topic_count_equally_divide]|
|defaultNamespaceBundleSplitAlgorithm| Default algorithm name for namespace bundle split |range_equally_divide|
|managedLedgerOffloadDriver| The directory for all the offloader implementations `offloadersDirectory=./offloaders`. Driver to use to offload old data to long term storage (Possible values: S3, aws-s3, google-cloud-storage). When using google-cloud-storage, Make sure both Google Cloud Storage and Google Cloud Storage JSON API are enabled for the project (check from Developers Console -> Api&auth -> APIs). ||
|managedLedgerOffloadMaxThreads|  Maximum number of thread pool threads for ledger offloading |2|
|managedLedgerOffloadPrefetchRounds|The maximum prefetch rounds for ledger reading for offloading.|1|
|managedLedgerUnackedRangesOpenCacheSetEnabled|  Use Open Range-Set to cache unacknowledged messages |true|
|managedLedgerOffloadDeletionLagMs|Delay between a ledger being successfully offloaded to long term storage and the ledger being deleted from bookkeeper | 14400000|
|managedLedgerOffloadAutoTriggerSizeThresholdBytes|The number of bytes before triggering automatic offload to long term storage |-1 (disabled)|
|s3ManagedLedgerOffloadRegion|  For Amazon S3 ledger offload, AWS region  ||
|s3ManagedLedgerOffloadBucket|  For Amazon S3 ledger offload, Bucket to place offloaded ledger into ||
|s3ManagedLedgerOffloadServiceEndpoint| For Amazon S3 ledger offload, Alternative endpoint to connect to (useful for testing) ||
|s3ManagedLedgerOffloadMaxBlockSizeInBytes| For Amazon S3 ledger offload, Max block size in bytes. (64MB by default, 5MB minimum) |67108864|
|s3ManagedLedgerOffloadReadBufferSizeInBytes| For Amazon S3 ledger offload, Read buffer size in bytes (1MB by default)  |1048576|
|gcsManagedLedgerOffloadRegion|For Google Cloud Storage ledger offload, region where offload bucket is located. Go to this page for more details: https://cloud.google.com/storage/docs/bucket-locations .|N/A|
|gcsManagedLedgerOffloadBucket|For Google Cloud Storage ledger offload, Bucket to place offloaded ledger into.|N/A|
|gcsManagedLedgerOffloadMaxBlockSizeInBytes|For Google Cloud Storage ledger offload, the maximum block size in bytes. (64MB by default, 5MB minimum)|67108864|
|gcsManagedLedgerOffloadReadBufferSizeInBytes|For Google Cloud Storage ledger offload, Read buffer size in bytes. (1MB by default)|1048576|
|gcsManagedLedgerOffloadServiceAccountKeyFile|For Google Cloud Storage, path to json file containing service account credentials. For more details, see the "Service Accounts" section of https://support.google.com/googleapi/answer/6158849 .|N/A|
|fileSystemProfilePath|For File System Storage, file system profile path.|../conf/filesystem_offload_core_site.xml|
|fileSystemURI|For File System Storage, file system uri.|N/A|
|s3ManagedLedgerOffloadRole| For Amazon S3 ledger offload, provide a role to assume before writing to s3 ||
|s3ManagedLedgerOffloadRoleSessionName| For Amazon S3 ledger offload, provide a role session name when using a role |pulsar-s3-offload|
| acknowledgmentAtBatchIndexLevelEnabled | Enable or disable the batch index acknowledgement. | false |
|enableReplicatedSubscriptions|Whether to enable tracking of replicated subscriptions state across clusters.|true|
|replicatedSubscriptionsSnapshotFrequencyMillis|The frequency of snapshots for replicated subscriptions tracking.|1000|
|replicatedSubscriptionsSnapshotTimeoutSeconds|The timeout for building a consistent snapshot for tracking replicated subscriptions state.|30|
|replicatedSubscriptionsSnapshotMaxCachedPerSubscription|The maximum number of snapshot to be cached per subscription.|10|
|maxMessagePublishBufferSizeInMB|The maximum memory size for broker handling messages sent from producers. If the processing message size exceeds this value, broker stops reading data from the connection. The processing messages means messages are sent to broker but broker have not sent response to the client. Usually the message are waiting to be written to bookies. It's shared across all the topics running in the same broker. The value `-1` disables the memory limitation. By default, it is 50% of direct memory.|N/A|
|messagePublishBufferCheckIntervalInMillis|Interval between checks to see if message publish buffer size exceeds the maximum. Use `0` or negative number to disable the max publish buffer limiting.|100|
|retentionCheckIntervalInSeconds|Check between intervals to see if consumed ledgers need to be trimmed. Use 0 or negative number to disable the check.|120|
| maxMessageSize | Set the maximum size of a message. | 5242880 |
| preciseTopicPublishRateLimiterEnable | Enable precise topic publish rate limiting. | false |
| lazyCursorRecovery | Whether to recover cursors lazily when trying to recover a managed ledger backing a persistent topic. It can improve write availability of topics. The caveat is now when recovered ledger is ready to write we're not sure if all old consumers' last mark delete position(ack position) can be recovered or not. So user can make the trade off or have custom logic in application to checkpoint consumer state.| false |  
|haProxyProtocolEnabled | Enable or disable the [HAProxy](http://www.haproxy.org/) protocol. |false|
|bookieId | If you want to custom a bookie ID or use a dynamic network address for the bookie, you can set this option. <br /><br />Bookie advertises itself using the `bookieId` rather than the `BookieSocketAddress` (`hostname:port` or `IP:port`).<br /><br /> The `bookieId` is a non-empty string that can contain ASCII digits and letters ([a-zA-Z9-0]), colons, dashes, and dots. <br /><br />For more information about `bookieId`, see [here](http://bookkeeper.apache.org/bps/BP-41-bookieid/).|N/A|

## Client

The [`pulsar-client`](reference-cli-tools.md#pulsar-client) CLI tool can be used to publish messages to Pulsar and consume messages from Pulsar topics. This tool can be used in lieu of a client library.

|Name|Description|Default|
|---|---|---|
|webServiceUrl| The web URL for the cluster.  |http://localhost:8080/|
|brokerServiceUrl|  The Pulsar protocol URL for the cluster.  |pulsar://localhost:6650/|
|authPlugin|  The authentication plugin.  ||
|authParams|  The authentication parameters for the cluster, as a comma-separated string. ||
|useTls|  Whether or not TLS authentication will be enforced in the cluster.  |false|
| tlsAllowInsecureConnection | Allow TLS connections to servers whose certificate cannot be verified to have been signed by a trusted certificate authority. | false |
| tlsEnableHostnameVerification | Whether the server hostname must match the common name of the certificate that is used by the server. | false |
|tlsTrustCertsFilePath|||
| useKeyStoreTls | Enable TLS with KeyStore type configuration in the broker. | false |
| tlsTrustStoreType | TLS TrustStore type configuration. <li>JKS </li><li>PKCS12 </li>|JKS|
| tlsTrustStore | TLS TrustStore path. | |
| tlsTrustStorePassword | TLS TrustStore password. | |


## Service discovery

|Name|Description|Default|
|---|---|---|
|zookeeperServers|  Zookeeper quorum connection string (comma-separated)  ||
|zooKeeperCacheExpirySeconds|ZooKeeper cache expiry time in seconds|300|
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
|zookeeperSessionTimeoutMs| ZooKeeper session timeout |30000|
|servicePort| Port to use to server binary-proto request  |6650|
|servicePortTls|  Port to use to server binary-proto-tls request  |6651|
|webServicePort|  Port that discovery service listen on |8080|
|webServicePortTls| Port to use to server HTTPS request |8443|
|bindOnLocalhost| Control whether to bind directly on localhost rather than on normal hostname  |false|
|authenticationEnabled| Enable authentication |false|
|authenticationProviders| Authentication provider name list, which is comma separated list of class names (comma-separated) ||
|authorizationEnabled|  Enforce authorization |false|
|superUserRoles|  Role names that are treated as “super-user”, meaning they will be able to do all admin operations and publish/consume from all topics (comma-separated) ||
|tlsEnabled|  Enable TLS  |false|
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||



## Log4j

|Name|Default|
|---|---|
|pulsar.root.logger|  WARN,CONSOLE|
|pulsar.log.dir|  logs|
|pulsar.log.file| pulsar.log|
|log4j.rootLogger|  ${pulsar.root.logger}|
|log4j.appender.CONSOLE|  org.apache.log4j.ConsoleAppender|
|log4j.appender.CONSOLE.Threshold|  DEBUG|
|log4j.appender.CONSOLE.layout| org.apache.log4j.PatternLayout|
|log4j.appender.CONSOLE.layout.ConversionPattern| %d{ISO8601} - %-5p - [%t:%C{1}@%L] - %m%n|
|log4j.appender.ROLLINGFILE|  org.apache.log4j.DailyRollingFileAppender|
|log4j.appender.ROLLINGFILE.Threshold|  DEBUG|
|log4j.appender.ROLLINGFILE.File| ${pulsar.log.dir}/${pulsar.log.file}|
|log4j.appender.ROLLINGFILE.layout| org.apache.log4j.PatternLayout|
|log4j.appender.ROLLINGFILE.layout.ConversionPattern| %d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n|
|log4j.appender.TRACEFILE|  org.apache.log4j.FileAppender|
|log4j.appender.TRACEFILE.Threshold|  TRACE|
|log4j.appender.TRACEFILE.File| pulsar-trace.log|
|log4j.appender.TRACEFILE.layout| org.apache.log4j.PatternLayout|
|log4j.appender.TRACEFILE.layout.ConversionPattern| %d{ISO8601} - %-5p [%t:%C{1}@%L][%x] - %m%n|

> Note: 'topic' in log4j2.appender is configurable. 
> - If you want to append all logs to a single topic, set the same topic name.
> - If you want to append logs to different topics, you can set different topic names. 

## Log4j shell

|Name|Default|
|---|---|
|bookkeeper.root.logger|  ERROR,CONSOLE|
|log4j.rootLogger|  ${bookkeeper.root.logger}|
|log4j.appender.CONSOLE|  org.apache.log4j.ConsoleAppender|
|log4j.appender.CONSOLE.Threshold|  DEBUG|
|log4j.appender.CONSOLE.layout| org.apache.log4j.PatternLayout|
|log4j.appender.CONSOLE.layout.ConversionPattern| %d{ABSOLUTE} %-5p %m%n|
|log4j.logger.org.apache.zookeeper| ERROR|
|log4j.logger.org.apache.bookkeeper|  ERROR|
|log4j.logger.org.apache.bookkeeper.bookie.BookieShell| INFO|


## Standalone

|Name|Description|Default|
|---|---|---|
|authenticateOriginalAuthData|  If this flag is set to `true`, the broker authenticates the original Auth data; else it just accepts the originalPrincipal and authorizes it (if required). |false|
|zookeeperServers|  The quorum connection string for local ZooKeeper  ||
|zooKeeperCacheExpirySeconds|ZooKeeper cache expiry time in seconds|300|
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
|brokerServicePort| The port on which the standalone broker listens for connections |6650|
|webServicePort|  The port used by the standalone broker for HTTP requests  |8080|
|bindAddress| The hostname or IP address on which the standalone service binds  |0.0.0.0|
|advertisedAddress| The hostname or IP address that the standalone service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getHostName()` is used.  ||
| numIOThreads | Number of threads to use for Netty IO | 2 * Runtime.getRuntime().availableProcessors() |
| numHttpServerThreads | Number of threads to use for HTTP requests processing | 2 * Runtime.getRuntime().availableProcessors()|
|isRunningStandalone|This flag controls features that are meant to be used when running in standalone mode.|N/A|
|clusterName| The name of the cluster that this broker belongs to. |standalone|
| failureDomainsEnabled | Enable cluster's failure-domain which can distribute brokers into logical region. | false |
|zooKeeperSessionTimeoutMillis| The ZooKeeper session timeout, in milliseconds. |30000|
|zooKeeperOperationTimeoutSeconds|ZooKeeper operation timeout in seconds.|30|
|brokerShutdownTimeoutMs| The time to wait for graceful broker shutdown. After this time elapses, the process will be killed. |60000|
|skipBrokerShutdownOnOOM| Flag to skip broker shutdown when broker handles Out of memory error. |false|
|backlogQuotaCheckEnabled|  Enable the backlog quota check, which enforces a specified action when the quota is reached.  |true|
|backlogQuotaCheckIntervalInSeconds|  How often to check for topics that have reached the backlog quota.  |60|
|backlogQuotaDefaultLimitGB| The default per-topic backlog quota limit. Being less than 0 means no limitation. By default, it is -1. |-1|
|ttlDurationDefaultInSeconds|  The default Time to Live (TTL) for namespaces if the TTL is not configured at namespace policies. When the value is set to `0`, TTL is disabled. By default, TTL is disabled. |0|
|brokerDeleteInactiveTopicsEnabled| Enable the deletion of inactive topics. If topics are not consumed for some while, these inactive topics might be cleaned up. Deleting inactive topics is enabled by default. The default period is 1 minute. |true|
|brokerDeleteInactiveTopicsFrequencySeconds|  How often to check for inactive topics, in seconds. |60|
| maxPendingPublishRequestsPerConnection | Maximum pending publish requests per connection to avoid keeping large number of pending requests in memory | 1000|
|messageExpiryCheckIntervalInMinutes| How often to proactively check and purged expired messages. |5|
|activeConsumerFailoverDelayTimeMillis| How long to delay rewinding cursor and dispatching messages when active consumer is changed.  |1000|
| subscriptionExpirationTimeMinutes | How long to delete inactive subscriptions from last consumption. When it is set to 0, inactive subscriptions are not deleted automatically | 0 |
| subscriptionRedeliveryTrackerEnabled | Enable subscription message redelivery tracker to send redelivery count to consumer. | true |
|subscriptionKeySharedEnable|Whether to enable the Key_Shared subscription.|true|
| subscriptionKeySharedUseConsistentHashing | In the Key_Shared subscription mode, with default AUTO_SPLIT mode, use splitting ranges or consistent hashing to reassign keys to new consumers. | false |
| subscriptionKeySharedConsistentHashingReplicaPoints | In the Key_Shared subscription mode, the number of points in the consistent-hashing ring. The greater the number, the more equal the assignment of keys to consumers. | 100 |
| subscriptionExpiryCheckIntervalInMinutes | How frequently to proactively check and purge expired subscription |5 |
| brokerDeduplicationEnabled | Set the default behavior for message deduplication in the broker. This can be overridden per-namespace. If it is enabled, the broker rejects messages that are already stored in the topic. | false |
| brokerDeduplicationMaxNumberOfProducers | Maximum number of producer information that it's going to be persisted for deduplication purposes | 10000 |
| brokerDeduplicationEntriesInterval | Number of entries after which a deduplication information snapshot is taken. A greater interval leads to less snapshots being taken though it would increase the topic recovery time, when the entries published after the snapshot need to be replayed. | 1000 |
| brokerDeduplicationProducerInactivityTimeoutMinutes | The time of inactivity (in minutes) after which the broker discards deduplication information related to a disconnected producer. | 360 |
| defaultNumberOfNamespaceBundles | When a namespace is created without specifying the number of bundles, this value is used as the default setting.| 4 |
|clientLibraryVersionCheckEnabled|  Enable checks for minimum allowed client library version. |false|
|clientLibraryVersionCheckAllowUnversioned| Allow client libraries with no version information  |true|
|statusFilePath|  The path for the file used to determine the rotation status for the broker when responding to service discovery health checks |/usr/local/apache/htdocs|
|maxUnackedMessagesPerConsumer| The maximum number of unacknowledged messages allowed to be received by consumers on a shared subscription. The broker will stop sending messages to a consumer once this limit is reached or until the consumer begins acknowledging messages. A value of 0 disables the unacked message limit check and thus allows consumers to receive messages without any restrictions. |50000|
|maxUnackedMessagesPerSubscription| The same as above, except per subscription rather than per consumer.  |200000|
| maxUnackedMessagesPerBroker | Maximum number of unacknowledged messages allowed per broker. Once this limit reaches, the broker stops dispatching messages to all shared subscriptions which has a higher number of unacknowledged messages until subscriptions start acknowledging messages back and unacknowledged messages count reaches to limit/2. When the value is set to 0, unacknowledged message limit check is disabled and broker does not block dispatchers. | 0 |
| maxUnackedMessagesPerSubscriptionOnBrokerBlocked | Once the broker reaches maxUnackedMessagesPerBroker limit, it blocks subscriptions which have higher unacknowledged messages than this percentage limit and subscription does not receive any new messages until that subscription acknowledges messages back. | 0.16 |
|maxNumPartitionsPerPartitionedTopic|Max number of partitions per partitioned topic. Use 0 or negative number to disable the check|0|
|zookeeperSessionExpiredPolicy|There are two policies when ZooKeeper session expired happens, "shutdown" and "reconnect". If it is set to "shutdown" policy, when ZooKeeper session expired happens, the broker is shutdown. If it is set to "reconnect" policy, the broker tries to reconnect to ZooKeeper server and re-register metadata to ZooKeeper. Note: the "reconnect" policy is an experiment feature.|shutdown|
| topicPublisherThrottlingTickTimeMillis | Tick time to schedule task that checks topic publish rate limiting across all topics. A lower value can improve accuracy while throttling publish but it uses more CPU to perform frequent check. (Disable publish throttling with value 0) | 10|
| brokerPublisherThrottlingTickTimeMillis | Tick time to schedule task that checks broker publish rate limiting across all topics. A lower value can improve accuracy while throttling publish but it uses more CPU to perform frequent check. When the value is set to 0, publish throttling is disabled. |50 |
| brokerPublisherThrottlingMaxMessageRate | Maximum rate (in 1 second) of messages allowed to publish for a broker if the message rate limiting is enabled. When the value is set to 0, message rate limiting is disabled. | 0|
| brokerPublisherThrottlingMaxByteRate | Maximum rate (in 1 second) of bytes allowed to publish for a broker if the  byte rate limiting is enabled. When the value is set to 0, the byte rate limiting is disabled. | 0 |
|subscribeThrottlingRatePerConsumer|Too many subscribe requests from a consumer can cause broker rewinding consumer cursors and loading data from bookies, hence causing high network bandwidth usage. When the positive value is set, broker will throttle the subscribe requests for one consumer. Otherwise, the throttling will be disabled. By default, throttling is disabled.|0|
|subscribeRatePeriodPerConsumerInSecond|Rate period for {subscribeThrottlingRatePerConsumer}. By default, it is 30s.|30|
| dispatchThrottlingRatePerTopicInMsg | Default messages (per second) dispatch throttling-limit for every topic. When the value is set to 0, default message dispatch throttling-limit is disabled. |0 |
| dispatchThrottlingRatePerTopicInByte | Default byte (per second) dispatch throttling-limit for every topic. When the value is set to 0, default byte dispatch throttling-limit is disabled. | 0|
| dispatchThrottlingRateRelativeToPublishRate | Enable dispatch rate-limiting relative to publish rate. | false |
|dispatchThrottlingRatePerSubscriptionInMsg|The defaulted number of message dispatching throttling-limit for a subscription. The value of 0 disables message dispatch-throttling.|0|
|dispatchThrottlingRatePerSubscriptionInByte|The default number of message-bytes dispatching throttling-limit for a subscription. The value of 0 disables message-byte dispatch-throttling.|0|
| dispatchThrottlingOnNonBacklogConsumerEnabled | Enable dispatch-throttling for both caught up consumers as well as consumers who have backlogs. | true |
|dispatcherMaxReadBatchSize|The maximum number of entries to read from BookKeeper. By default, it is 100 entries.|100|
|dispatcherMaxReadSizeBytes|The maximum size in bytes of entries to read from BookKeeper. By default, it is 5MB.|5242880|
|dispatcherMinReadBatchSize|The minimum number of entries to read from BookKeeper. By default, it is 1 entry. When there is an error occurred on reading entries from bookkeeper, the broker will backoff the batch size to this minimum number.|1|
|dispatcherMaxRoundRobinBatchSize|The maximum number of entries to dispatch for a shared subscription. By default, it is 20 entries.|20|
| preciseDispatcherFlowControl | Precise dispathcer flow control according to history message number of each entry. | false |
| maxConcurrentLookupRequest | Maximum number of concurrent lookup request that the broker allows to throttle heavy incoming lookup traffic. | 50000 |
| maxConcurrentTopicLoadRequest | Maximum number of concurrent topic loading request that the broker allows to control the number of zk-operations. | 5000 |
| maxConcurrentNonPersistentMessagePerConnection | Maximum number of concurrent non-persistent message that can be processed per connection. | 1000 |
| numWorkerThreadsForNonPersistentTopic | Number of worker threads to serve non-persistent topic. | 8 |
| enablePersistentTopics | Enable broker to load persistent topics. | true |
| enableNonPersistentTopics | Enable broker to load non-persistent topics. | true |
| maxProducersPerTopic | Maximum number of producers allowed to connect to topic. Once this limit reaches, the broker rejects new producers until the number of connected producers decreases. When the value is set to 0, maxProducersPerTopic-limit check is disabled. | 0 |
| maxConsumersPerTopic | Maximum number of consumers allowed to connect to topic. Once this limit reaches, the broker rejects new consumers until the number of connected consumers decreases. When the value is set to 0, maxConsumersPerTopic-limit check is disabled. | 0 |
| maxConsumersPerSubscription | Maximum number of consumers allowed to connect to subscription. Once this limit reaches, the broker rejects new consumers until the number of connected consumers decreases. When the value is set to 0, maxConsumersPerSubscription-limit check is disabled. | 0 |
| maxNumPartitionsPerPartitionedTopic | Maximum number of partitions per partitioned topic. When the value is set to a negative number or is set to 0, the check is disabled. | 0 |
| tlsCertRefreshCheckDurationSec | TLS certificate refresh duration in seconds. When the value is set to 0, check the TLS certificate on every new connection. | 300 |
| tlsCertificateFilePath | Path for the TLS certificate file. | |
| tlsKeyFilePath | Path for the TLS private key file. | |
| tlsTrustCertsFilePath | Path for the trusted TLS certificate file.| |
| tlsAllowInsecureConnection | Accept untrusted TLS certificate from the client. If it is set to true, a client with a certificate which cannot be verified with the 'tlsTrustCertsFilePath' certificate is allowed to connect to the server, though the certificate is not be used for client authentication. | false |
| tlsProtocols | Specify the TLS protocols the broker uses to negotiate during TLS handshake. | |
| tlsCiphers | Specify the TLS cipher the broker uses to negotiate during TLS Handshake. | |
| tlsRequireTrustedClientCertOnConnect | Trusted client certificates are required for to connect TLS. Reject the Connection if the client certificate is not trusted. In effect, this requires that all connecting clients perform TLS client authentication. | false |
| tlsEnabledWithKeyStore | Enable TLS with KeyStore type configuration in broker. | false |
| tlsProvider | TLS Provider for KeyStore type. | |
| tlsKeyStoreType | TLS KeyStore type configuration in the broker.<li>JKS </li><li>PKCS12 </li>|JKS|
| tlsKeyStore | TLS KeyStore path in the broker. | |
| tlsKeyStorePassword | TLS KeyStore password for the broker. | |
| tlsTrustStoreType | TLS TrustStore type configuration in the broker<li>JKS </li><li>PKCS12 </li>|JKS|
| tlsTrustStore | TLS TrustStore path in the broker. | |
| tlsTrustStorePassword | TLS TrustStore password for the broker. | |
| brokerClientTlsEnabledWithKeyStore | Configure whether the internal client uses the KeyStore type to authenticate with Pulsar brokers. | false |
| brokerClientSslProvider | The TLS Provider used by the internal client to authenticate with other Pulsar brokers. | |
| brokerClientTlsTrustStoreType | TLS TrustStore type configuration for the internal client to authenticate with Pulsar brokers. <li>JKS </li><li>PKCS12 </li>| JKS |
| brokerClientTlsTrustStore | TLS TrustStore path for the internal client to authenticate with Pulsar brokers. | |
| brokerClientTlsTrustStorePassword | TLS TrustStore password for the internal client to authenticate with Pulsar brokers. | |
| brokerClientTlsCiphers | Specify the TLS cipher that the internal client uses to negotiate during TLS Handshake. | |
| brokerClientTlsProtocols | Specify the TLS protocols that the broker uses to negotiate during TLS handshake. | |
| systemTopicEnabled | Enable/Disable system topics. | false |
| topicLevelPoliciesEnabled | Enable or disable topic level policies. Topic level policies depends on the system topic. Please enable the system topic first. | false |
| proxyRoles | Role names that are treated as "proxy roles". If the broker sees a request with role as proxyRoles, it demands to see a valid original principal. | |
| authenticateOriginalAuthData | If this flag is set, the broker authenticates the original Auth data. Otherwise, it just accepts the originalPrincipal and authorizes it (if required). | false |
|authenticationEnabled| Enable authentication for the broker. |false|
|authenticationProviders| A comma-separated list of class names for authentication providers. |false|
|authorizationEnabled|  Enforce authorization in brokers. |false|
| authorizationProvider | Authorization provider fully qualified class-name. | org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider |
| authorizationAllowWildcardsMatching | Allow wildcard matching in authorization. Wildcard matching is applicable only when the wildcard-character (*) presents at the **first** or **last** position. | false |
|superUserRoles|  Role names that are treated as “superusers.” Superusers are authorized to perform all admin tasks. | |
|brokerClientAuthenticationPlugin|  The authentication settings of the broker itself. Used when the broker connects to other brokers either in the same cluster or from other clusters. | |
|brokerClientAuthenticationParameters|  The parameters that go along with the plugin specified using brokerClientAuthenticationPlugin.  | |
|athenzDomainNames| Supported Athenz authentication provider domain names as a comma-separated list.  | |
| anonymousUserRole | When this parameter is not empty, unauthenticated users perform as anonymousUserRole. | |
|tokenAuthClaim| Specify the token claim that will be used as the authentication "principal" or "role". The "subject" field will be used if this is left blank ||
|tokenAudienceClaim| The token audience "claim" name, e.g. "aud". It is used to get the audience from token. If it is not set, the audience is not verified. ||
| tokenAudience | The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token need contains this parameter.| |
|saslJaasClientAllowedIds|This is a regexp, which limits the range of possible ids which can connect to the Broker using SASL. By default, it is set to `SaslConstants.JAAS_CLIENT_ALLOWED_IDS_DEFAULT`, which is ".*pulsar.*", so only clients whose id contains 'pulsar' are allowed to connect.|N/A|
|saslJaasBrokerSectionName|Service Principal, for login context name. By default, it is set to `SaslConstants.JAAS_DEFAULT_BROKER_SECTION_NAME`, which is "Broker".|N/A|
|httpMaxRequestSize|If the value is larger than 0, it rejects all HTTP requests with bodies larged than the configured limit.|-1|
|exposePreciseBacklogInPrometheus| Enable expose the precise backlog stats, set false to use published counter and consumed counter to calculate, this would be more efficient but may be inaccurate. |false|
|bookkeeperMetadataServiceUri|Metadata service uri is what BookKeeper used for loading corresponding metadata driver and resolving its metadata service location. This value can be fetched using `bookkeeper shell whatisinstanceid` command in BookKeeper cluster. For example: `zk+hierarchical://localhost:2181/ledgers`. The metadata service uri list can also be semicolon separated values like: `zk+hierarchical://zk1:2181;zk2:2181;zk3:2181/ledgers`.|N/A|
|bookkeeperClientAuthenticationPlugin|  Authentication plugin to be used when connecting to bookies (BookKeeper servers). ||
|bookkeeperClientAuthenticationParametersName|  BookKeeper authentication plugin implementation parameters and values.  ||
|bookkeeperClientAuthenticationParameters|  Parameters associated with the bookkeeperClientAuthenticationParametersName ||
|bookkeeperClientTimeoutInSeconds|  Timeout for BookKeeper add and read operations. |30|
|bookkeeperClientSpeculativeReadTimeoutInMillis|  Speculative reads are initiated if a read request doesn’t complete within a certain time. A value of 0 disables speculative reads.  |0|
|bookkeeperUseV2WireProtocol|Use older Bookkeeper wire protocol with bookie.|true|
|bookkeeperClientHealthCheckEnabled|  Enable bookie health checks.  |true|
|bookkeeperClientHealthCheckIntervalSeconds|  The time interval, in seconds, at which health checks are performed. New ledgers are not created during health checks.  |60|
|bookkeeperClientHealthCheckErrorThresholdPerInterval|  Error threshold for health checks.  |5|
|bookkeeperClientHealthCheckQuarantineTimeInSeconds|  If bookies have more than the allowed number of failures within the time interval specified by bookkeeperClientHealthCheckIntervalSeconds |1800|
|bookkeeperClientGetBookieInfoIntervalSeconds|Specify options for the GetBookieInfo check. This setting helps ensure the list of bookies that are up to date on the brokers.|86400|
|bookkeeperClientGetBookieInfoRetryIntervalSeconds|Specify options for the GetBookieInfo check. This setting helps ensure the list of bookies that are up to date on the brokers.|60|
|bookkeeperClientRackawarePolicyEnabled|    |true|
|bookkeeperClientRegionawarePolicyEnabled|    |false|
|bookkeeperClientMinNumRacksPerWriteQuorum|    |2|
|bookkeeperClientMinNumRacksPerWriteQuorum|    |false|
|bookkeeperClientReorderReadSequenceEnabled|    |false|
|bookkeeperClientIsolationGroups|||
|bookkeeperClientSecondaryIsolationGroups| Enable bookie secondary-isolation group if bookkeeperClientIsolationGroups doesn't have enough bookie available.  ||
|bookkeeperClientMinAvailableBookiesInIsolationGroups| Minimum bookies that should be available as part of bookkeeperClientIsolationGroups else broker will include bookkeeperClientSecondaryIsolationGroups bookies in isolated list.  ||
| bookkeeperTLSProviderFactoryClass | Set the client security provider factory class name. | org.apache.bookkeeper.tls.TLSContextFactory |
| bookkeeperTLSClientAuthentication | Enable TLS authentication with bookie. | false |
| bookkeeperTLSKeyFileType | Supported type: PEM, JKS, PKCS12.  | PEM |
| bookkeeperTLSTrustCertTypes | Supported type: PEM, JKS, PKCS12.  | PEM |
| bookkeeperTLSKeyStorePasswordPath | Path to file containing keystore password, if the client keystore is password protected. | |
| bookkeeperTLSTrustStorePasswordPath | Path to file containing truststore password, if the client truststore is password protected. | |
| bookkeeperTLSKeyFilePath | Path for the TLS private key file. | |
| bookkeeperTLSCertificateFilePath | Path for the TLS certificate file. | |
| bookkeeperTLSTrustCertsFilePath | Path for the trusted TLS certificate file. | |
| bookkeeperDiskWeightBasedPlacementEnabled | Enable/Disable disk weight based placement. | false |
| bookkeeperExplicitLacIntervalInMills | Set the interval to check the need for sending an explicit LAC. When the value is set to 0, no explicit LAC is sent. | 0 |
| bookkeeperClientExposeStatsToPrometheus | Expose BookKeeper client managed ledger stats to Prometheus. | false |
|managedLedgerDefaultEnsembleSize|    |1|
|managedLedgerDefaultWriteQuorum|   |1|
|managedLedgerDefaultAckQuorum|   |1|
| managedLedgerDigestType | Default type of checksum to use when writing to BookKeeper. | CRC32C |
| managedLedgerNumWorkerThreads | Number of threads to be used for managed ledger tasks dispatching. | 8 |
| managedLedgerNumSchedulerThreads | Number of threads to be used for managed ledger scheduled tasks. | 8 |
|managedLedgerCacheSizeMB|    |N/A|
|managedLedgerCacheCopyEntries| Whether to copy the entry payloads when inserting in cache.| false|
|managedLedgerCacheEvictionWatermark|   |0.9|
|managedLedgerCacheEvictionFrequency| Configure the cache eviction frequency for the managed ledger cache (evictions/sec) | 100.0 |
|managedLedgerCacheEvictionTimeThresholdMillis| All entries that have stayed in cache for more than the configured time, will be evicted | 1000 |
|managedLedgerCursorBackloggedThreshold| Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged' and thus should be set as inactive. | 1000|
|managedLedgerUnackedRangesOpenCacheSetEnabled|  Use Open Range-Set to cache unacknowledged messages |true|
|managedLedgerDefaultMarkDeleteRateLimit|   |0.1|
|managedLedgerMaxEntriesPerLedger|    |50000|
|managedLedgerMinLedgerRolloverTimeMinutes|   |10|
|managedLedgerMaxLedgerRolloverTimeMinutes|   |240|
|managedLedgerCursorMaxEntriesPerLedger|    |50000|
|managedLedgerCursorRolloverTimeInSeconds|    |14400|
| managedLedgerMaxSizePerLedgerMbytes | Maximum ledger size before triggering a rollover for a topic. | 2048 |
| managedLedgerMaxUnackedRangesToPersist | Maximum number of "acknowledgment holes" that are going to be persistently stored. When acknowledging out of order, a consumer leaves holes that are supposed to be quickly filled by acknowledging all the messages. The information of which messages are acknowledged is persisted by compressing in "ranges" of messages that were acknowledged. After the max number of ranges is reached, the information is only tracked in memory and messages are redelivered in case of crashes. | 10000 |
| managedLedgerMaxUnackedRangesToPersistInZooKeeper | Maximum number of "acknowledgment holes" that can be stored in Zookeeper. If the number of unacknowledged message range is higher than this limit, the broker persists unacknowledged ranges into bookkeeper to avoid additional data overhead into Zookeeper. | 1000 |
|autoSkipNonRecoverableData|    |false|
| managedLedgerMetadataOperationsTimeoutSeconds | Operation timeout while updating managed-ledger metadata. | 60 |
| managedLedgerReadEntryTimeoutSeconds | Read entries timeout when the broker tries to read messages from BookKeeper. | 0 |
| managedLedgerAddEntryTimeoutSeconds | Add entry timeout when the broker tries to publish message to BookKeeper. | 0 |
| managedLedgerNewEntriesCheckDelayInMillis | New entries check delay for the cursor under the managed ledger. If no new messages in the topic, the cursor tries to check again after the delay time. For consumption latency sensitive scenario, you can set the value to a smaller value or 0. Of course, a smaller value may degrade consumption throughput.|10 ms|
| managedLedgerPrometheusStatsLatencyRolloverSeconds | Managed ledger prometheus stats latency rollover seconds.  | 60 |
| managedLedgerTraceTaskExecution | Whether to trace managed ledger task execution time. | true |
|managedLedgerNewEntriesCheckDelayInMillis|New entries check delay for the cursor under the managed ledger. If no new messages in the topic, the cursor will try to check again after the delay time. For consumption latency sensitive scenario, it can be set to a smaller value or 0. A smaller value degrades consumption throughput. By default, it is 10ms.|10|
|loadBalancerEnabled|   |false|
|loadBalancerPlacementStrategy|   |weightedRandomSelection|
|loadBalancerReportUpdateThresholdPercentage|   |10|
|loadBalancerReportUpdateMaxIntervalMinutes|    |15|
|loadBalancerHostUsageCheckIntervalMinutes|  |1|
|loadBalancerSheddingIntervalMinutes|   |30|
|loadBalancerSheddingGracePeriodMinutes|    |30|
|loadBalancerBrokerMaxTopics|   |50000|
|loadBalancerBrokerUnderloadedThresholdPercentage|    |1|
|loadBalancerBrokerOverloadedThresholdPercentage|   |85|
|loadBalancerResourceQuotaUpdateIntervalMinutes|    |15|
|loadBalancerBrokerComfortLoadLevelPercentage|    |65|
|loadBalancerAutoBundleSplitEnabled|    |false|
| loadBalancerAutoUnloadSplitBundlesEnabled | Enable/Disable automatic unloading of split bundles. | true |
|loadBalancerNamespaceBundleMaxTopics|    |1000|
|loadBalancerNamespaceBundleMaxSessions|    |1000|
|loadBalancerNamespaceBundleMaxMsgRate|   |1000|
|loadBalancerNamespaceBundleMaxBandwidthMbytes|   |100|
|loadBalancerNamespaceMaximumBundles|   |128|
| loadBalancerBrokerThresholdShedderPercentage | The broker resource usage threshold. When the broker resource usage is greater than the pulsar cluster average resource usage, the threshold shedder is triggered to offload bundles from the broker. It only takes effect in the ThresholdShedder strategy. | 10 |
| loadBalancerHistoryResourcePercentage | The history usage when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 0.9 |
| loadBalancerBandwithInResourceWeight | The BandWithIn usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 1.0 |
| loadBalancerBandwithOutResourceWeight | The BandWithOut usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 1.0 |
| loadBalancerCPUResourceWeight | The CPU usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 1.0 |
| loadBalancerMemoryResourceWeight | The heap memory usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 1.0 |
| loadBalancerDirectMemoryResourceWeight | The direct memory usage weight when calculating new resource usage. It only takes effect in the ThresholdShedder strategy. | 1.0 |
| loadBalancerBundleUnloadMinThroughputThreshold | Bundle unload minimum throughput threshold. Avoid bundle unload frequently. It only takes effect in the ThresholdShedder strategy. | 10 |
|replicationMetricsEnabled|   |true|
|replicationConnectionsPerBroker|   |16|
|replicationProducerQueueSize|    |1000|
| replicationPolicyCheckDurationSeconds | Duration to check replication policy to avoid replicator inconsistency due to missing ZooKeeper watch. When the value is set to 0, disable checking replication policy. | 600 |
|defaultRetentionTimeInMinutes|   |0|
|defaultRetentionSizeInMB|    |0|
|keepAliveIntervalSeconds|    |30|
|haProxyProtocolEnabled | Enable or disable the [HAProxy](http://www.haproxy.org/) protocol. |false|




## WebSocket

|Name|Description|Default|
|---|---|---|
|configurationStoreServers    |||
|zooKeeperSessionTimeoutMillis|   |30000|
|zooKeeperCacheExpirySeconds|ZooKeeper cache expiry time in seconds|300|
|serviceUrl|||
|serviceUrlTls|||
|brokerServiceUrl|||
|brokerServiceUrlTls|||
|webServicePort||8080|
|webServicePortTls||8443|
|bindAddress||0.0.0.0|
|clusterName |||
|authenticationEnabled||false|
|authenticationProviders|||
|authorizationEnabled||false|
|superUserRoles |||
|brokerClientAuthenticationPlugin|||
|brokerClientAuthenticationParameters|||
|tlsEnabled||false|
|tlsAllowInsecureConnection||false|
|tlsCertificateFilePath|||
|tlsKeyFilePath |||
|tlsTrustCertsFilePath|||


## Pulsar proxy

The [Pulsar proxy](concepts-architecture-overview.md#pulsar-proxy) can be configured in the `conf/proxy.conf` file.


|Name|Description|Default|
|---|---|---|
|forwardAuthorizationCredentials| Forward client authorization credentials to Broker for re-authorization, and make sure authentication is enabled for this to take effect. |false|
|zookeeperServers|  The ZooKeeper quorum connection string (as a comma-separated list)  ||
|configurationStoreServers| Configuration store connection string (as a comma-separated list) ||
| brokerServiceURL | The service URL pointing to the broker cluster. | |
| brokerServiceURLTLS | The TLS service URL pointing to the broker cluster | |
| brokerWebServiceURL | The Web service URL pointing to the broker cluster | |
| brokerWebServiceURLTLS | The TLS Web service URL pointing to the broker cluster | |
| functionWorkerWebServiceURL | The Web service URL pointing to the function worker cluster. It is only configured when you setup function workers in a separate cluster. | |
| functionWorkerWebServiceURLTLS | The TLS Web service URL pointing to the function worker cluster. It is only configured when you setup function workers in a separate cluster. | |
|zookeeperSessionTimeoutMs| ZooKeeper session timeout (in milliseconds) |30000|
|zooKeeperCacheExpirySeconds|ZooKeeper cache expiry time in seconds|300|
|advertisedAddress|Hostname or IP address the service advertises to the outside world. If not set, the value of `InetAddress.getLocalHost().getHostname()` is used.|N/A|
|servicePort| The port to use for server binary Protobuf requests |6650|
|servicePortTls|  The port to use to server binary Protobuf TLS requests  |6651|
|statusFilePath|  Path for the file used to determine the rotation status for the proxy instance when responding to service discovery health checks ||
|advertisedAddress|Hostname or IP address the service advertises to the outside world.|`InetAddress.getLocalHost().getHostname()`|
| proxyLogLevel | Proxy log level <li>0: Do not log any TCP channel information. </li><li>1: Parse and log any TCP channel information and command information without message body. </li><li>2: Parse and log channel information, command information and message body.</li>| 0 |
|authenticationEnabled| Whether authentication is enabled for the Pulsar proxy  |false|
|authenticateMetricsEndpoint| Whether the '/metrics' endpoint requires authentication. Defaults to true. 'authenticationEnabled' must also be set for this to take effect. |true|
|authenticationProviders| Authentication provider name list (a comma-separated list of class names) ||
|authorizationEnabled|  Whether authorization is enforced by the Pulsar proxy |false|
|authorizationProvider| Authorization provider as a fully qualified class name  |org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider|
| anonymousUserRole | When this parameter is not empty, unauthenticated users perform as anonymousUserRole. | |
|brokerClientAuthenticationPlugin|  The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientAuthenticationParameters|  The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers  ||
|brokerClientTrustCertsFilePath|  The path to trusted certificates used by the Pulsar proxy to authenticate with Pulsar brokers ||
|superUserRoles|  Role names that are treated as “super-users,” meaning that they will be able to perform all admin ||
|maxConcurrentInboundConnections| Max concurrent inbound connections. The proxy will reject requests beyond that. |10000|
|maxConcurrentLookupRequests| Max concurrent outbound connections. The proxy will error out requests beyond that. |50000|
|tlsEnabledInProxy| Deprecated - use `servicePortTls` and `webServicePortTls` instead. |false|
|tlsEnabledWithBroker|  Whether TLS is enabled when communicating with Pulsar brokers. |false|
| tlsCertRefreshCheckDurationSec | TLS certificate refresh duration in seconds. If the value is set 0, check TLS certificate every new connection. | 300 |
|tlsCertificateFilePath|  Path for the TLS certificate file ||
|tlsKeyFilePath|  Path for the TLS private key file ||
|tlsTrustCertsFilePath| Path for the trusted TLS certificate pem file ||
|tlsHostnameVerificationEnabled|  Whether the hostname is validated when the proxy creates a TLS connection with brokers  |false|
|tlsRequireTrustedClientCertOnConnect|  Whether client certificates are required for TLS. Connections are rejected if the client certificate isn’t trusted. |false|
|tlsProtocols|Specify the tls protocols the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLSv1.2```, ```TLSv1.1```, ```TLSv1``` ||
|tlsCiphers|Specify the tls cipher the broker will use to negotiate during TLS Handshake. Multiple values can be specified, separated by commas. Example:- ```TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256```||
| httpReverseProxyConfigs | HTTP directs to redirect to non-pulsar services | |
| httpOutputBufferSize | HTTP output buffer size. The amount of data that will be buffered for HTTP requests before it is flushed to the channel. A larger buffer size may result in higher HTTP throughput though it may take longer for the client to see data. If using HTTP streaming via the reverse proxy, this should be set to the minimum value (1) so that clients see the data as soon as possible. | 32768 |
| httpNumThreads | Number of threads to use for HTTP requests processing|  2 * Runtime.getRuntime().availableProcessors() |
|tokenSecretKey| Configure the secret key to be used to validate auth tokens. The key can be specified like: `tokenSecretKey=data:;base64,xxxxxxxxx` or `tokenSecretKey=file:///my/secret.key`||
|tokenPublicKey| Configure the public key to be used to validate auth tokens. The key can be specified like: `tokenPublicKey=data:;base64,xxxxxxxxx` or `tokenPublicKey=file:///my/secret.key`||
|tokenAuthClaim| Specify the token claim that will be used as the authentication "principal" or "role". The "subject" field will be used if this is left blank ||
|tokenAudienceClaim| The token audience "claim" name, e.g. "aud". It is used to get the audience from token. If it is not set, the audience is not verified. ||
| tokenAudience | The token audience stands for this broker. The field `tokenAudienceClaim` of a valid token need contains this parameter.| |
|haProxyProtocolEnabled | Enable or disable the [HAProxy](http://www.haproxy.org/) protocol. |false|

## ZooKeeper

ZooKeeper handles a broad range of essential configuration- and coordination-related tasks for Pulsar. The default configuration file for ZooKeeper is in the `conf/zookeeper.conf` file in your Pulsar installation. The following parameters are available:


|Name|Description|Default|
|---|---|---|
|tickTime|  The tick is the basic unit of time in ZooKeeper, measured in milliseconds and used to regulate things like heartbeats and timeouts. tickTime is the length of a single tick.  |2000|
|initLimit| The maximum time, in ticks, that the leader ZooKeeper server allows follower ZooKeeper servers to successfully connect and sync. The tick time is set in milliseconds using the tickTime parameter. |10|
|syncLimit| The maximum time, in ticks, that a follower ZooKeeper server is allowed to sync with other ZooKeeper servers. The tick time is set in milliseconds using the tickTime parameter.  |5|
|dataDir| The location where ZooKeeper will store in-memory database snapshots as well as the transaction log of updates to the database. |data/zookeeper|
|clientPort|  The port on which the ZooKeeper server will listen for connections. |2181|
|admin.enableServer|The port at which the admin listens.|true|
|admin.serverPort|The port at which the admin listens.|9990|
|autopurge.snapRetainCount| In ZooKeeper, auto purge determines how many recent snapshots of the database stored in dataDir to retain within the time interval specified by autopurge.purgeInterval (while deleting the rest).  |3|
|autopurge.purgeInterval| The time interval, in hours, by which the ZooKeeper database purge task is triggered. Setting to a non-zero number will enable auto purge; setting to 0 will disable. Read this guide before enabling auto purge. |1|
|forceSync|Requires updates to be synced to media of the transaction log before finishing processing the update. If this option is set to 'no', ZooKeeper will not require updates to be synced to the media. WARNING: it's not recommended to run a production ZK cluster with `forceSync` disabled.|yes|
|maxClientCnxns|  The maximum number of client connections. Increase this if you need to handle more ZooKeeper clients. |60|




In addition to the parameters in the table above, configuring ZooKeeper for Pulsar involves adding
a `server.N` line to the `conf/zookeeper.conf` file for each node in the ZooKeeper cluster, where `N` is the number of the ZooKeeper node. Here's an example for a three-node ZooKeeper cluster:

```properties

server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888

```

> We strongly recommend consulting the [ZooKeeper Administrator's Guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html) for a more thorough and comprehensive introduction to ZooKeeper configuration
