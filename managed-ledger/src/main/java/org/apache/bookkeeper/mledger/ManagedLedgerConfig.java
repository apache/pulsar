/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Charsets;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.impl.NullLedgerOffloader;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.pulsar.common.util.collections.ConcurrentOpenLongPairRangeSet;

/**
 * Configuration class for a ManagedLedger.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public class ManagedLedgerConfig {

    private boolean createIfMissing = true;
    private int maxUnackedRangesToPersist = 10000;
    private int maxBatchDeletedIndexToPersist = 10000;
    private boolean deletionAtBatchIndexLevelEnabled = true;
    private int maxUnackedRangesToPersistInZk = 1000;
    private int maxEntriesPerLedger = 50000;
    private int maxSizePerLedgerMb = 100;
    private int minimumRolloverTimeMs = 0;
    private long maximumRolloverTimeMs = TimeUnit.HOURS.toMillis(4);
    private int ensembleSize = 3;
    private int writeQuorumSize = 2;
    private int ackQuorumSize = 2;
    private int metadataEnsembleSize = 3;
    private int metadataWriteQuorumSize = 2;
    private int metadataAckQuorumSize = 2;
    private int metadataMaxEntriesPerLedger = 50000;
    private int ledgerRolloverTimeout = 4 * 3600;
    private double throttleMarkDelete = 0;
    private long retentionTimeMs = 0;
    private long retentionSizeInMB = 0;
    private boolean autoSkipNonRecoverableData;
    private boolean lazyCursorRecovery = false;
    private long metadataOperationsTimeoutSeconds = 60;
    private long readEntryTimeoutSeconds = 120;
    private long addEntryTimeoutSeconds = 120;
    private DigestType digestType = DigestType.CRC32C;
    private byte[] password = "".getBytes(Charsets.UTF_8);
    private boolean unackedRangesOpenCacheSetEnabled = true;
    private Class<? extends EnsemblePlacementPolicy>  bookKeeperEnsemblePlacementPolicyClassName;
    private Map<String, Object> bookKeeperEnsemblePlacementPolicyProperties;
    private LedgerOffloader ledgerOffloader = NullLedgerOffloader.INSTANCE;
    private int newEntriesCheckDelayInMillis = 10;
    private Clock clock = Clock.systemUTC();
    private ManagedLedgerInterceptor managedLedgerInterceptor;
    private Map<String, String> properties;
    private int inactiveLedgerRollOverTimeMs = 0;
    @Getter
    @Setter
    private boolean cacheEvictionByMarkDeletedPosition = false;

    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    public ManagedLedgerConfig setCreateIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
        return this;
    }

    /**
     * @return the lazyCursorRecovery
     */
    public boolean isLazyCursorRecovery() {
        return lazyCursorRecovery;
    }

    /**
     * Whether to recover cursors lazily when trying to recover a
     * managed ledger backing a persistent topic. It can improve write availability of topics.
     * The caveat is now when recovered ledger is ready to write we're not sure if all old consumers last mark
     * delete position can be recovered or not.
     * @param lazyCursorRecovery if enable lazy cursor recovery.
     */
    public ManagedLedgerConfig setLazyCursorRecovery(boolean lazyCursorRecovery) {
        this.lazyCursorRecovery = lazyCursorRecovery;
        return this;
    }

    /**
     * @return the maxEntriesPerLedger
     */
    public int getMaxEntriesPerLedger() {
        return maxEntriesPerLedger;
    }

    /**
     * @param maxEntriesPerLedger
     *            the maxEntriesPerLedger to set
     */
    public ManagedLedgerConfig setMaxEntriesPerLedger(int maxEntriesPerLedger) {
        this.maxEntriesPerLedger = maxEntriesPerLedger;
        return this;
    }

    /**
     * @return the maxSizePerLedgerMb
     */
    public int getMaxSizePerLedgerMb() {
        return maxSizePerLedgerMb;
    }

    /**
     * @param maxSizePerLedgerMb
     *            the maxSizePerLedgerMb to set
     */
    public ManagedLedgerConfig setMaxSizePerLedgerMb(int maxSizePerLedgerMb) {
        this.maxSizePerLedgerMb = maxSizePerLedgerMb;
        return this;
    }

    /**
     * @return the minimum rollover time
     */
    public int getMinimumRolloverTimeMs() {
        return minimumRolloverTimeMs;
    }

    /**
     * Set the minimum rollover time for ledgers in this managed ledger.
     *
     * <p/>If this time is > 0, a ledger will not be rolled over more frequently than the specified time, even if it has
     * reached the maximum number of entries or maximum size. This parameter can be used to reduce the amount of
     * rollovers on managed ledger with high write throughput.
     *
     * @param minimumRolloverTime
     *            the minimum rollover time
     * @param unit
     *            the time unit
     */
    public void setMinimumRolloverTime(int minimumRolloverTime, TimeUnit unit) {
        this.minimumRolloverTimeMs = (int) unit.toMillis(minimumRolloverTime);
        checkArgument(maximumRolloverTimeMs >= minimumRolloverTimeMs,
                "Minimum rollover time needs to be less than maximum rollover time");
    }

    /**
     * @return the maximum rollover time.
     */
    public long getMaximumRolloverTimeMs() {
        return maximumRolloverTimeMs;
    }

    /**
     * Set the maximum rollover time for ledgers in this managed ledger.
     *
     * <p/>If the ledger is not rolled over until this time, even if it has not reached the number of entry or size
     * limit, this setting will trigger rollover. This parameter can be used for topics with low request rate to force
     * rollover, so recovery failure does not have to go far back.
     *
     * @param maximumRolloverTime
     *            the maximum rollover time
     * @param unit
     *            the time unit
     */
    public void setMaximumRolloverTime(int maximumRolloverTime, TimeUnit unit) {
        this.maximumRolloverTimeMs = unit.toMillis(maximumRolloverTime);
        checkArgument(maximumRolloverTimeMs >= minimumRolloverTimeMs,
                "Maximum rollover time needs to be greater than minimum rollover time");
    }

    /**
     * @return the ensembleSize
     */
    public int getEnsembleSize() {
        return ensembleSize;
    }

    /**
     * @param ensembleSize
     *            the ensembleSize to set
     */
    public ManagedLedgerConfig setEnsembleSize(int ensembleSize) {
        this.ensembleSize = ensembleSize;
        return this;
    }

    /**
     * @return the ackQuorumSize
     */
    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    /**
     * @return the writeQuorumSize
     */
    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    /**
     * @param writeQuorumSize
     *            the writeQuorumSize to set
     */
    public ManagedLedgerConfig setWriteQuorumSize(int writeQuorumSize) {
        this.writeQuorumSize = writeQuorumSize;
        return this;
    }

    /**
     * @param ackQuorumSize
     *            the ackQuorumSize to set
     */
    public ManagedLedgerConfig setAckQuorumSize(int ackQuorumSize) {
        this.ackQuorumSize = ackQuorumSize;
        return this;
    }

    /**
     * @return the digestType
     */
    public DigestType getDigestType() {
        return digestType;
    }

    /**
     * @param digestType
     *            the digestType to set
     */
    public ManagedLedgerConfig setDigestType(DigestType digestType) {
        this.digestType = digestType;
        return this;
    }

    /**
     * @return the password
     */
    public byte[] getPassword() {
        return Arrays.copyOf(password, password.length);
    }

    /**
     * @param password
     *            the password to set
     */
    public ManagedLedgerConfig setPassword(String password) {
        this.password = password.getBytes(Charsets.UTF_8);
        return this;
    }

    /**
     * should use {@link ConcurrentOpenLongPairRangeSet} to store unacked ranges.
     * @return
     */
    public boolean isUnackedRangesOpenCacheSetEnabled() {
        return unackedRangesOpenCacheSetEnabled;
    }

    public ManagedLedgerConfig setUnackedRangesOpenCacheSetEnabled(boolean unackedRangesOpenCacheSetEnabled) {
        this.unackedRangesOpenCacheSetEnabled = unackedRangesOpenCacheSetEnabled;
        return this;
    }

    /**
     * @return the metadataEnsemblesize
     */
    public int getMetadataEnsemblesize() {
        return metadataEnsembleSize;
    }

    /**
     * @param metadataEnsembleSize
     *            the metadataEnsembleSize to set
     */
    public ManagedLedgerConfig setMetadataEnsembleSize(int metadataEnsembleSize) {
        this.metadataEnsembleSize = metadataEnsembleSize;
        return this;
    }

    /**
     * @return the metadataAckQuorumSize
     */
    public int getMetadataAckQuorumSize() {
        return metadataAckQuorumSize;
    }

    /**
     * @return the metadataWriteQuorumSize
     */
    public int getMetadataWriteQuorumSize() {
        return metadataWriteQuorumSize;
    }

    /**
     * @param metadataAckQuorumSize
     *            the metadataAckQuorumSize to set
     */
    public ManagedLedgerConfig setMetadataAckQuorumSize(int metadataAckQuorumSize) {
        this.metadataAckQuorumSize = metadataAckQuorumSize;
        return this;
    }

    /**
     * @param metadataWriteQuorumSize
     *            the metadataWriteQuorumSize to set
     */
    public ManagedLedgerConfig setMetadataWriteQuorumSize(int metadataWriteQuorumSize) {
        this.metadataWriteQuorumSize = metadataWriteQuorumSize;
        return this;
    }

    /**
     * @return the metadataMaxEntriesPerLedger
     */
    public int getMetadataMaxEntriesPerLedger() {
        return metadataMaxEntriesPerLedger;
    }

    /**
     * @param metadataMaxEntriesPerLedger
     *            the metadataMaxEntriesPerLedger to set
     */
    public ManagedLedgerConfig setMetadataMaxEntriesPerLedger(int metadataMaxEntriesPerLedger) {
        this.metadataMaxEntriesPerLedger = metadataMaxEntriesPerLedger;
        return this;
    }

    /**
     * @return the ledgerRolloverTimeout
     */
    public int getLedgerRolloverTimeout() {
        return ledgerRolloverTimeout;
    }

    /**
     * @param ledgerRolloverTimeout
     *            the ledgerRolloverTimeout to set
     */
    public ManagedLedgerConfig setLedgerRolloverTimeout(int ledgerRolloverTimeout) {
        this.ledgerRolloverTimeout = ledgerRolloverTimeout;
        return this;
    }

    /**
     * @return the throttling rate limit for mark-delete calls
     */
    public double getThrottleMarkDelete() {
        return throttleMarkDelete;
    }

    /**
     * Set the rate limiter on how many mark-delete calls per second are allowed. If the value is set to 0, the rate
     * limiter is disabled. Default is 0.
     *
     * @param throttleMarkDelete
     *            the max number of mark-delete calls allowed per second
     */
    public ManagedLedgerConfig setThrottleMarkDelete(double throttleMarkDelete) {
        checkArgument(throttleMarkDelete >= 0.0);
        this.throttleMarkDelete = throttleMarkDelete;
        return this;
    }

    /**
     * Set the retention time for the ManagedLedger.
     * <p>
     * Retention time and retention size ({@link #setRetentionSizeInMB(long)}) are together used to retain the
     * ledger data when when there are no cursors or when all the cursors have marked the data for deletion.
     * Data will be deleted in this case when both retention time and retention size settings don't prevent deleting
     * the data marked for deletion.
     * <p>
     * A retention time of 0 (default) will make data to be deleted immediately.
     * <p>
     * A retention time of -1, means to have an unlimited retention time.
     *
     * @param retentionTime
     *            duration for which messages should be retained
     * @param unit
     *            time unit for retention time
     */
    public ManagedLedgerConfig setRetentionTime(int retentionTime, TimeUnit unit) {
        this.retentionTimeMs = unit.toMillis(retentionTime);
        return this;
    }

    /**
     * @return duration for which messages are retained
     *
     */
    public long getRetentionTimeMillis() {
        return retentionTimeMs;
    }

    /**
     * The retention size is used to set a maximum retention size quota on the ManagedLedger.
     * <p>
     * Retention size and retention time ({@link #setRetentionTime(int, TimeUnit)}) are together used to retain the
     * ledger data when when there are no cursors or when all the cursors have marked the data for deletion.
     * Data will be deleted in this case when both retention time and retention size settings don't prevent deleting
     * the data marked for deletion.
     * <p>
     * A retention size of 0 (default) will make data to be deleted immediately.
     * <p>
     * A retention size of -1, means to have an unlimited retention size.
     *
     * @param retentionSizeInMB
     *            quota for message retention
     */
    public ManagedLedgerConfig setRetentionSizeInMB(long retentionSizeInMB) {
        this.retentionSizeInMB = retentionSizeInMB;
        return this;
    }

    /**
     * @return quota for message retention
     *
     */
    public long getRetentionSizeInMB() {
        return retentionSizeInMB;
    }


    /**
     * Skip reading non-recoverable/unreadable data-ledger under managed-ledger's list. It helps when data-ledgers gets
     * corrupted at bookkeeper and managed-cursor is stuck at that ledger.
     */
    public boolean isAutoSkipNonRecoverableData() {
        return autoSkipNonRecoverableData;
    }

    public void setAutoSkipNonRecoverableData(boolean skipNonRecoverableData) {
        this.autoSkipNonRecoverableData = skipNonRecoverableData;
    }

    /**
     * @return max unacked message ranges that will be persisted and recovered.
     *
     */
    public int getMaxUnackedRangesToPersist() {
        return maxUnackedRangesToPersist;
    }

    /**
     * @return max batch deleted index that will be persisted and recoverd.
     */
    public int getMaxBatchDeletedIndexToPersist() {
        return maxBatchDeletedIndexToPersist;
    }

    /**
     * @param maxUnackedRangesToPersist
     *            max unacked message ranges that will be persisted and receverd.
     */
    public ManagedLedgerConfig setMaxUnackedRangesToPersist(int maxUnackedRangesToPersist) {
        this.maxUnackedRangesToPersist = maxUnackedRangesToPersist;
        return this;
    }

    /**
     * @return max unacked message ranges up to which it can store in Zookeeper
     *
     */
    public int getMaxUnackedRangesToPersistInZk() {
        return maxUnackedRangesToPersistInZk;
    }

    public void setMaxUnackedRangesToPersistInZk(int maxUnackedRangesToPersistInZk) {
        this.maxUnackedRangesToPersistInZk = maxUnackedRangesToPersistInZk;
    }

    /**
     * Get ledger offloader which will be used to offload ledgers to longterm storage.
     *
     * The default offloader throws an exception on any attempt to offload.
     *
     * @return a ledger offloader
     */
    public LedgerOffloader getLedgerOffloader() {
        return ledgerOffloader;
    }

    /**
     * Set ledger offloader to use for offloading ledgers to longterm storage.
     *
     * @param offloader the ledger offloader to use
     */
    public ManagedLedgerConfig setLedgerOffloader(LedgerOffloader offloader) {
        this.ledgerOffloader = offloader;
        return this;
    }

    /**
     * Get clock to use to time operations.
     *
     * @return a clock
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * Set clock to use for time operations.
     *
     * @param clock the clock to use
     */
    public ManagedLedgerConfig setClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    /**
     *
     * Ledger-Op (Create/Delete) timeout.
     *
     * @return
     */
    public long getMetadataOperationsTimeoutSeconds() {
        return metadataOperationsTimeoutSeconds;
    }

    /**
     * Ledger-Op (Create/Delete) timeout after which callback will be completed with failure.
     *
     * @param metadataOperationsTimeoutSeconds
     */
    public ManagedLedgerConfig setMetadataOperationsTimeoutSeconds(long metadataOperationsTimeoutSeconds) {
        this.metadataOperationsTimeoutSeconds = metadataOperationsTimeoutSeconds;
        return this;
    }

    /**
     * Ledger read-entry timeout.
     *
     * @return
     */
    public long getReadEntryTimeoutSeconds() {
        return readEntryTimeoutSeconds;
    }

    /**
     * Ledger read entry timeout after which callback will be completed with failure. (disable timeout by setting.
     * readTimeoutSeconds <= 0)
     *
     * @param readEntryTimeoutSeconds
     * @return
     */
    public ManagedLedgerConfig setReadEntryTimeoutSeconds(long readEntryTimeoutSeconds) {
        this.readEntryTimeoutSeconds = readEntryTimeoutSeconds;
        return this;
    }

    public long getAddEntryTimeoutSeconds() {
        return addEntryTimeoutSeconds;
    }

    /**
     * Add-entry timeout after which add-entry callback will be failed if add-entry is not succeeded.
     *
     * @param addEntryTimeoutSeconds
     */
    public ManagedLedgerConfig setAddEntryTimeoutSeconds(long addEntryTimeoutSeconds) {
        this.addEntryTimeoutSeconds = addEntryTimeoutSeconds;
        return this;
    }

    /**
     * Managed-ledger can setup different custom EnsemblePlacementPolicy (eg: affinity to write ledgers to only setup of
     * group of bookies).
     *
     * @return
     */
    public Class<? extends EnsemblePlacementPolicy> getBookKeeperEnsemblePlacementPolicyClassName() {
        return bookKeeperEnsemblePlacementPolicyClassName;
    }

    /**
     * Returns EnsemblePlacementPolicy configured for the Managed-ledger.
     *
     * @param bookKeeperEnsemblePlacementPolicyClassName
     */
    public void setBookKeeperEnsemblePlacementPolicyClassName(
            Class<? extends EnsemblePlacementPolicy> bookKeeperEnsemblePlacementPolicyClassName) {
        this.bookKeeperEnsemblePlacementPolicyClassName = bookKeeperEnsemblePlacementPolicyClassName;
    }

    /**
     * Returns properties required by configured bookKeeperEnsemblePlacementPolicy.
     *
     * @return
     */
    public Map<String, Object> getBookKeeperEnsemblePlacementPolicyProperties() {
        return bookKeeperEnsemblePlacementPolicyProperties;
    }

    /**
     * Managed-ledger can setup different custom EnsemblePlacementPolicy which needs
     * bookKeeperEnsemblePlacementPolicy-Properties.
     *
     * @param bookKeeperEnsemblePlacementPolicyProperties
     */
    public void setBookKeeperEnsemblePlacementPolicyProperties(
            Map<String, Object> bookKeeperEnsemblePlacementPolicyProperties) {
        this.bookKeeperEnsemblePlacementPolicyProperties = bookKeeperEnsemblePlacementPolicyProperties;
    }


    public Map<String, String> getProperties() {
        return properties;
    }


    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public boolean isDeletionAtBatchIndexLevelEnabled() {
        return deletionAtBatchIndexLevelEnabled;
    }

    public void setDeletionAtBatchIndexLevelEnabled(boolean deletionAtBatchIndexLevelEnabled) {
        this.deletionAtBatchIndexLevelEnabled = deletionAtBatchIndexLevelEnabled;
    }

    public int getNewEntriesCheckDelayInMillis() {
        return newEntriesCheckDelayInMillis;
    }

    public void setNewEntriesCheckDelayInMillis(int newEntriesCheckDelayInMillis) {
        this.newEntriesCheckDelayInMillis = newEntriesCheckDelayInMillis;
    }

    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return managedLedgerInterceptor;
    }

    public void setManagedLedgerInterceptor(ManagedLedgerInterceptor managedLedgerInterceptor) {
        this.managedLedgerInterceptor = managedLedgerInterceptor;
    }

    public int getInactiveLedgerRollOverTimeMs() {
        return inactiveLedgerRollOverTimeMs;
    }

    /**
     * Set rollOver time for inactive ledgers.
     *
     * @param inactiveLedgerRollOverTimeMs
     * @param unit
     */
    public void setInactiveLedgerRollOverTime(int inactiveLedgerRollOverTimeMs, TimeUnit unit) {
        this.inactiveLedgerRollOverTimeMs = (int) unit.toMillis(inactiveLedgerRollOverTimeMs);
    }

}
