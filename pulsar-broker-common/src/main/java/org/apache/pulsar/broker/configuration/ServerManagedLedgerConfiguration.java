package org.apache.pulsar.broker.configuration;

import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_STORAGE_ML;
import static org.apache.pulsar.broker.ServiceConfiguration.CATEGORY_STORAGE_OFFLOADING;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.DirectMemoryUtils;


@Getter
@Setter
@ToString
public class ServerManagedLedgerConfiguration implements PulsarConfiguration {
    private ServiceConfiguration serviceConfiguration;

    /**** --- Managed Ledger. --- ****/
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Ensemble (E) size, Number of bookies to use for storing entries in a ledger.\n"
                    + "Please notice that sticky reads enabled by bookkeeperEnableStickyReads=true aren’t used "
                    + " unless ensemble size (E) equals write quorum (Qw) size."
    )
    private int managedLedgerDefaultEnsembleSize = 2;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Write quorum (Qw) size, Replication factor for storing entries (messages) in a ledger."
    )
    private int managedLedgerDefaultWriteQuorum = 2;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Ack quorum (Qa) size, Number of guaranteed copies "
                    + "(acks to wait for before a write is considered completed)"
    )
    private int managedLedgerDefaultAckQuorum = 2;

    @FieldContext(minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "How frequently to flush the cursor positions that were accumulated due to rate limiting. (seconds)."
                    + " Default is 60 seconds")
    private int managedLedgerCursorPositionFlushSeconds = 60;

    @FieldContext(minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "How frequently to refresh the stats. (seconds). Default is 60 seconds")
    private int managedLedgerStatsPeriodSeconds = 60;

    //
    //
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Default type of checksum to use when writing to BookKeeper. \n\nDefault is `CRC32C`."
                    + " Other possible options are `CRC32`, `MAC` or `DUMMY` (no checksum)."
    )
    private DigestType managedLedgerDigestType = DigestType.CRC32C;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Default  password to use when writing to BookKeeper. \n\nDefault is ``."
    )
    @ToString.Exclude
    private String managedLedgerPassword = "";

    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of bookies to use when creating a ledger"
    )
    private int managedLedgerMaxEnsembleSize = 5;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of copies to store for each message"
    )
    private int managedLedgerMaxWriteQuorum = 5;
    @FieldContext(
            minValue = 1,
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of guaranteed copies (acks to wait before write is complete)"
    )
    private int managedLedgerMaxAckQuorum = 5;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "Amount of memory to use for caching data payload in managed ledger. \n\nThis"
                    + " memory is allocated from JVM direct memory and it's shared across all the topics"
                    + " running in the same broker. By default, uses 1/5th of available direct memory")
    private int managedLedgerCacheSizeMB = Math.max(64,
            (int) (DirectMemoryUtils.jvmMaxDirectMemory() / 5 / (1024 * 1024)));

    @FieldContext(category = CATEGORY_STORAGE_ML, doc = "Whether we should make a copy of the entry payloads when "
            + "inserting in cache")
    private boolean managedLedgerCacheCopyEntries = false;

    @FieldContext(category = CATEGORY_STORAGE_ML, doc = "Maximum buffer size for bytes read from storage."
            + " This is the memory retained by data read from storage (or cache) until it has been delivered to the"
            + " Consumer Netty channel. Use O to disable")
    private long managedLedgerMaxReadsInFlightSizeInMB = 0;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "Threshold to which bring down the cache level when eviction is triggered"
    )
    private double managedLedgerCacheEvictionWatermark = 0.9;
    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Configure the cache eviction frequency for the managed ledger cache.")
    @Deprecated
    private double managedLedgerCacheEvictionFrequency = 0;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Configure the cache eviction interval in milliseconds for the managed ledger cache, default is 10ms")
    private long managedLedgerCacheEvictionIntervalMs = 10;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "All entries that have stayed in cache for more than the configured time, will be evicted")
    private long managedLedgerCacheEvictionTimeThresholdMillis = 1000;
    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Configure the threshold (in number of entries) from where a cursor should be considered 'backlogged'"
                    + " and thus should be set as inactive.")
    private long managedLedgerCursorBackloggedThreshold = 1000;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Rate limit the amount of writes per second generated by consumer acking the messages"
    )
    private double managedLedgerDefaultMarkDeleteRateLimit = 1.0;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "Allow automated creation of topics if set to true (default value)."
    )
    private boolean allowAutoTopicCreation = true;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "The type of topic that is allowed to be automatically created.(partitioned/non-partitioned)"
    )
    private TopicType allowAutoTopicCreationType = TopicType.NON_PARTITIONED;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "Allow automated creation of subscriptions if set to true (default value)."
    )
    private boolean allowAutoSubscriptionCreation = true;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            dynamic = true,
            doc = "The number of partitioned topics that is allowed to be automatically created"
                    + " if allowAutoTopicCreationType is partitioned."
    )
    private int defaultNumPartitions = 1;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "The class of the managed ledger storage"
    )
    private String managedLedgerStorageClassName = "org.apache.pulsar.broker.ManagedLedgerClientFactory";
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Number of threads to be used for managed ledger scheduled tasks"
    )
    private int managedLedgerNumSchedulerThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of entries to append to a ledger before triggering a rollover.\n\n"
                    + "A ledger rollover is triggered after the min rollover time has passed"
                    + " and one of the following conditions is true:"
                    + " the max rollover time has been reached,"
                    + " the max entries have been written to the ledger, or"
                    + " the max ledger size has been written to the ledger")
    private int managedLedgerMaxEntriesPerLedger = 50000;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Minimum time between ledger rollover for a topic"
    )
    private int managedLedgerMinLedgerRolloverTimeMinutes = 10;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Maximum time before forcing a ledger rollover for a topic"
    )
    private int managedLedgerMaxLedgerRolloverTimeMinutes = 240;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Maximum ledger size before triggering a rollover for a topic (MB)"
    )
    private int managedLedgerMaxSizePerLedgerMbytes = 2048;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of entries to append to a cursor ledger"
    )
    private int managedLedgerCursorMaxEntriesPerLedger = 50000;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Max time before triggering a rollover on a cursor ledger"
    )
    private int managedLedgerCursorRolloverTimeInSeconds = 14400;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of `acknowledgment holes` that are going to be persistently stored.\n\n"
                    + "When acknowledging out of order, a consumer will leave holes that are supposed"
                    + " to be quickly filled by acking all the messages. The information of which"
                    + " messages are acknowledged is persisted by compressing in `ranges` of messages"
                    + " that were acknowledged. After the max number of ranges is reached, the information"
                    + " will only be tracked in memory and messages will be redelivered in case of"
                    + " crashes.")
    private int managedLedgerMaxUnackedRangesToPersist = 10000;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "If enabled, the maximum \"acknowledgment holes\" will not be limited and \"acknowledgment holes\" "
                    + "are stored in multiple entries.")
    private boolean persistentUnackedRangesWithMultipleEntriesEnabled = false;
    @Deprecated
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            deprecated = true,
            doc = "Max number of `acknowledgment holes` that can be stored in Zookeeper.\n\n"
                    + "If number of unack message range is higher than this limit then broker will persist"
                    + " unacked ranges into bookkeeper to avoid additional data overhead into zookeeper.\n"
                    + "@deprecated - use managedLedgerMaxUnackedRangesToPersistInMetadataStore.")
    private int managedLedgerMaxUnackedRangesToPersistInZooKeeper = -1;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Max number of `acknowledgment holes` that can be stored in MetadataStore.\n\n"
                    + "If number of unack message range is higher than this limit then broker will persist"
                    + " unacked ranges into bookkeeper to avoid additional data overhead into MetadataStore.")
    private int managedLedgerMaxUnackedRangesToPersistInMetadataStore = 1000;
    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Use Open Range-Set to cache unacked messages (it is memory efficient but it can take more cpu)"
    )
    private boolean managedLedgerUnackedRangesOpenCacheSetEnabled = true;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Skip reading non-recoverable/unreadable data-ledger under managed-ledger's list.\n\n"
                    + " It helps when data-ledgers gets corrupted "
                    + "at bookkeeper and managed-cursor is stuck at that ledger."
    )
    private boolean autoSkipNonRecoverableData = false;
    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "operation timeout while updating managed-ledger metadata."
    )
    private long managedLedgerMetadataOperationsTimeoutSeconds = 60;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Read entries timeout when broker tries to read messages from bookkeeper "
                    + "(0 to disable it)"
    )
    private long managedLedgerReadEntryTimeoutSeconds = 0;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Add entry timeout when broker tries to publish message to bookkeeper.(0 to disable it)")
    private long managedLedgerAddEntryTimeoutSeconds = 0;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Managed ledger prometheus stats latency rollover seconds"
    )
    private int managedLedgerPrometheusStatsLatencyRolloverSeconds = 60;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Whether trace managed ledger task execution time"
    )
    private boolean managedLedgerTraceTaskExecution = true;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "New entries check delay for the cursor under the managed ledger. \n"
                    + "If no new messages in the topic, the cursor will try to check again after the delay time. \n"
                    + "For consumption latency sensitive scenario, can set to a smaller value or set to 0.\n"
                    + "Of course, this may degrade consumption throughput. Default is 10ms.")
    private int managedLedgerNewEntriesCheckDelayInMillis = 10;

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "Read priority when ledgers exists in both bookkeeper and the second layer storage.")
    private String managedLedgerDataReadPriority = OffloadedReadPriority.TIERED_STORAGE_FIRST
            .getValue();

    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "ManagedLedgerInfo compression type, option values (NONE, LZ4, ZLIB, ZSTD, SNAPPY). \n"
                    + "If value is invalid or NONE, then save the ManagedLedgerInfo bytes data directly.")
    private String managedLedgerInfoCompressionType = "NONE";


    @FieldContext(category = CATEGORY_STORAGE_ML,
            doc = "ManagedCursorInfo compression type, option values (NONE, LZ4, ZLIB, ZSTD, SNAPPY). \n"
                    + "If value is NONE, then save the ManagedCursorInfo bytes data directly.")
    private String managedCursorInfoCompressionType = "NONE";

    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Minimum cursors that must be in backlog state to cache and reuse the read entries."
                    + "(Default =0 to disable backlog reach cache)"
    )
    private int managedLedgerMinimumBacklogCursorsForCaching = 0;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Minimum backlog entries for any cursor before start caching reads"
    )
    private int managedLedgerMinimumBacklogEntriesForCaching = 1000;
    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Maximum backlog entry difference to prevent caching entries that can't be reused"
    )
    private int managedLedgerMaxBacklogBetweenCursorsForCaching = 1000;

    @FieldContext(
            dynamic = true,
            category = CATEGORY_STORAGE_ML,
            doc = "Time to rollover ledger for inactive topic (duration without any publish on that topic). "
                    + "Disable rollover with value 0 (Default value 0)"
    )
    private int managedLedgerInactiveLedgerRolloverTimeSeconds = 0;

    @FieldContext(
            category = CATEGORY_STORAGE_ML,
            doc = "Evicting cache data by the slowest markDeletedPosition or readPosition. "
                    + "The default is to evict through readPosition."
    )
    private boolean cacheEvictionByMarkDeletedPosition = false;


    /**** --- Ledger Offloading. --- ****/
    /****
     * NOTES: all implementation related settings should be put in implementation package.
     *        only common settings like driver name, io threads can be added here.
     ****/
    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "The directory to locate offloaders"
    )
    private String offloadersDirectory = "./offloaders";

    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Driver to use to offload old data to long term storage"
    )
    private String managedLedgerOffloadDriver = null;

    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Maximum number of thread pool threads for ledger offloading"
    )
    private int managedLedgerOffloadMaxThreads = 2;

    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "The directory where nar Extraction of offloaders happens"
    )
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Maximum prefetch rounds for ledger reading for offloading"
    )
    private int managedLedgerOffloadPrefetchRounds = 1;

    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "Delay between a ledger being successfully offloaded to long term storage,"
                    + " and the ledger being deleted from bookkeeper"
    )
    private long managedLedgerOffloadDeletionLagMs = TimeUnit.HOURS.toMillis(4);
    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "The number of bytes before triggering automatic offload to long term storage"
    )
    private long managedLedgerOffloadAutoTriggerSizeThresholdBytes = -1L;
    @FieldContext(
            category = CATEGORY_STORAGE_OFFLOADING,
            doc = "The threshold to triggering automatic offload to long term storage"
    )
    private long managedLedgerOffloadThresholdInSeconds = -1L;


    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
