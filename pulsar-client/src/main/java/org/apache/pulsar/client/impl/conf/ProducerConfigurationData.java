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
package org.apache.pulsar.client.impl.conf;

import java.io.Serializable;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Data;

import static com.google.common.base.Preconditions.checkArgument;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerConfigurationData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_BATCHING_MAX_MESSAGES = 1000;
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 1000;
    public static final int DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 50000;

    private String topicName = null;
    private String producerName = null;
    private long sendTimeoutMs = 30000;
    private boolean blockIfQueueFull = false;
    private int maxPendingMessages = DEFAULT_MAX_PENDING_MESSAGES;
    private int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
    private MessageRoutingMode messageRoutingMode = null;
    private HashingScheme hashingScheme = HashingScheme.JavaStringHash;

    private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

    @JsonIgnore
    private MessageRouter customMessageRouter = null;

    private long batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1);
    private int batchingPartitionSwitchFrequencyByPublishDelay = 10;
    private int batchingMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;
    private int batchingMaxBytes = 128 * 1024; // 128KB (keep the maximum consistent as previous versions)
    private boolean batchingEnabled = true; // enabled by default
    @JsonIgnore
    private BatcherBuilder batcherBuilder = BatcherBuilder.DEFAULT;
    private boolean chunkingEnabled = false;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader;

    @JsonIgnore
    private transient MessageCrypto messageCrypto = null;

    @JsonIgnore
    private Set<String> encryptionKeys = new TreeSet<>();

    private CompressionType compressionType = CompressionType.NONE;

    // Cannot use Optional<Long> since it's not serializable
    private Long initialSequenceId = null;

    private boolean autoUpdatePartitions = true;

    private long autoUpdatePartitionsIntervalSeconds = 60;

    private boolean multiSchema = true;

    private ProducerAccessMode accessMode = ProducerAccessMode.Shared;

    private boolean lazyStartPartitionedProducers = false;

    private SortedMap<String, String> properties = new TreeMap<>();

    /**
     *
     * Returns true if encryption keys are added
     *
     */
    @JsonIgnore
    public boolean isEncryptionEnabled() {
        return (this.encryptionKeys != null) && !this.encryptionKeys.isEmpty() && (this.cryptoKeyReader != null);
    }

    public ProducerConfigurationData clone() {
        try {
            ProducerConfigurationData c = (ProducerConfigurationData) super.clone();
            c.encryptionKeys = Sets.newTreeSet(this.encryptionKeys);
            c.properties = Maps.newTreeMap(this.properties);
            return c;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ProducerConfigurationData", e);
        }
    }

    public void setProducerName(String producerName) {
        checkArgument(StringUtils.isNotBlank(producerName), "producerName cannot be blank");
        this.producerName = producerName;
    }

    public void setMaxPendingMessages(int maxPendingMessages) {
        checkArgument(maxPendingMessages >= 0, "maxPendingMessages needs to be >= 0");
        this.maxPendingMessages = maxPendingMessages;
    }

    public void setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
        checkArgument(maxPendingMessagesAcrossPartitions >= maxPendingMessages,
                "maxPendingMessagesAcrossPartitions needs to be >= maxPendingMessages");
        this.maxPendingMessagesAcrossPartitions = maxPendingMessagesAcrossPartitions;
    }

    public void setBatchingMaxMessages(int batchingMaxMessages) {
        this.batchingMaxMessages = batchingMaxMessages;
    }

    public void setBatchingMaxBytes(int batchingMaxBytes) {
        this.batchingMaxBytes = batchingMaxBytes;
    }

    public void setSendTimeoutMs(int sendTimeout, TimeUnit timeUnit) {
        checkArgument(sendTimeout >= 0, "sendTimeout needs to be >= 0");
        this.sendTimeoutMs = timeUnit.toMillis(sendTimeout);
    }

    public void setBatchingMaxPublishDelayMicros(long batchDelay, TimeUnit timeUnit) {
        long delayInMs = timeUnit.toMillis(batchDelay);
        checkArgument(delayInMs >= 1, "configured value for batch delay must be at least 1ms");
        this.batchingMaxPublishDelayMicros = timeUnit.toMicros(batchDelay);
    }

    public void setBatchingPartitionSwitchFrequencyByPublishDelay(int frequencyByPublishDelay) {
        checkArgument(frequencyByPublishDelay >= 1, "configured value for partition switch frequency must be >= 1");
        this.batchingPartitionSwitchFrequencyByPublishDelay = frequencyByPublishDelay;
    }

    public long batchingPartitionSwitchFrequencyIntervalMicros() {
        return this.batchingPartitionSwitchFrequencyByPublishDelay * batchingMaxPublishDelayMicros;
    }

    public void setAutoUpdatePartitionsIntervalSeconds(int interval, TimeUnit timeUnit) {
        checkArgument(interval > 0, "interval needs to be > 0");
        this.autoUpdatePartitionsIntervalSeconds = timeUnit.toSeconds(interval);
    }
}
