/*
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

import static com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
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

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerConfigurationData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_BATCHING_MAX_MESSAGES = 1000;
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 0;
    public static final int DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS = 0;

    @ApiModelProperty(
            name = "topicName",
            required = true,
            value = "Topic name"
    )
    private String topicName = null;

    @ApiModelProperty(
            name = "producerName",
            value = "Producer name"
    )
    private String producerName = null;

    @ApiModelProperty(
            name = "sendTimeoutMs",
            value = "Message send timeout in ms.\n"
                    + "If a message is not acknowledged by a server before the `sendTimeout` expires, an error occurs."
    )
    private long sendTimeoutMs = 30000;

    @ApiModelProperty(
            name = "blockIfQueueFull",
            value = "If it is set to `true`, when the outgoing message queue is full, the `Send` and `SendAsync`"
                    + " methods of producer block, rather than failing and throwing errors.\n"
                    + "If it is set to `false`, when the outgoing message queue is full, the `Send` and `SendAsync`"
                    + " methods of producer fail and `ProducerQueueIsFullError` exceptions occur.\n"
                    + "\n"
                    + "The `MaxPendingMessages` parameter determines the size of the outgoing message queue."
    )
    private boolean blockIfQueueFull = false;

    @ApiModelProperty(
            name = "maxPendingMessages",
            value = "The maximum size of a queue holding pending messages.\n"
                    + "\n"
                    + "For example, a message waiting to receive an acknowledgment from a [broker]"
                    + "(https://pulsar.apache.org/docs/reference-terminology#broker).\n"
                    + "\n"
                    + "By default, when the queue is full, all calls to the `Send` and `SendAsync` methods fail"
                    + " **unless** you set `BlockIfQueueFull` to `true`."
    )
    private int maxPendingMessages = DEFAULT_MAX_PENDING_MESSAGES;

    @ApiModelProperty(
            name = "maxPendingMessagesAcrossPartitions",
            value = "The maximum number of pending messages across partitions.\n"
                    + "\n"
                    + "Use the setting to lower the max pending messages for each partition ({@link "
                    + "#setMaxPendingMessages(int)}) if the total number exceeds the configured value."
    )
    private int maxPendingMessagesAcrossPartitions = DEFAULT_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;

    @ApiModelProperty(
            name = "messageRoutingMode",
            value = "Message routing logic for producers on [partitioned topics]"
                    + "(https://pulsar.apache.org/docs/concepts-architecture-overview#partitioned-topics).\n"
                    + "Apply the logic only when setting no key on messages.\n"
                    + "Available options are as follows:\n"
                    + "* `pulsar.RoundRobinDistribution`: round robin\n"
                    + "* `pulsar.UseSinglePartition`: publish all messages to a single partition\n"
                    + "* `pulsar.CustomPartition`: a custom partitioning scheme"
    )
    private MessageRoutingMode messageRoutingMode = null;

    @ApiModelProperty(
            name = "hashingScheme",
            value = "Hashing function determining the partition where you publish a particular message (partitioned "
                    + "topics only).\n"
                    + "Available options are as follows:\n"
                    + "* `pulsar.JavastringHash`: the equivalent of `string.hashCode()` in Java\n"
                    + "* `pulsar.Murmur3_32Hash`: applies the [Murmur3](https://en.wikipedia.org/wiki/MurmurHash)"
                    + " hashing function\n"
                    + "* `pulsar.BoostHash`: applies the hashing function from C++'s"
                    + "[Boost](https://www.boost.org/doc/libs/1_62_0/doc/html/hash.html) library"
    )
    private HashingScheme hashingScheme = HashingScheme.JavaStringHash;

    @ApiModelProperty(
            name = "cryptoFailureAction",
            value = "Producer should take action when encryption fails.\n"
                    + "* **FAIL**: if encryption fails, unencrypted messages fail to send.\n"
                    + "* **SEND**: if encryption fails, unencrypted messages are sent."
    )
    private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

    @JsonIgnore
    private MessageRouter customMessageRouter = null;

    @ApiModelProperty(
            name = "batchingMaxPublishDelayMicros",
            value = "Batching time period of sending messages."
    )
    private long batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1);
    private int batchingPartitionSwitchFrequencyByPublishDelay = 10;

    @ApiModelProperty(
            name = "batchingMaxMessages",
            value = "The maximum number of messages permitted in a batch."
    )
    private int batchingMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;
    private int batchingMaxBytes = 128 * 1024; // 128KB (keep the maximum consistent as previous versions)

    @ApiModelProperty(
            name = "batchingEnabled",
            value = "Enable batching of messages."
    )
    private boolean batchingEnabled = true; // enabled by default
    @JsonIgnore
    private BatcherBuilder batcherBuilder = BatcherBuilder.DEFAULT;

    @ApiModelProperty(
            name = "chunkingEnabled",
            value = "Enable chunking of messages."
    )
    private boolean chunkingEnabled = false;
    private int chunkMaxMessageSize = -1;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader;

    @JsonIgnore
    private transient MessageCrypto messageCrypto = null;

    private Set<String> encryptionKeys = new TreeSet<>();

    @ApiModelProperty(
            name = "compressionType",
            value = "Message data compression type used by a producer.\n"
                    + "Available options:\n"
                    + "* [LZ4](https://github.com/lz4/lz4)\n"
                    + "* [ZLIB](https://zlib.net/)\n"
                    + "* [ZSTD](https://facebook.github.io/zstd/)\n"
                    + "* [SNAPPY](https://google.github.io/snappy/)"
    )
    private CompressionType compressionType = CompressionType.NONE;

    // Cannot use Optional<Long> since it's not serializable
    private Long initialSequenceId = null;

    private boolean autoUpdatePartitions = true;

    private long autoUpdatePartitionsIntervalSeconds = 60;

    private boolean multiSchema = true;

    private ProducerAccessMode accessMode = ProducerAccessMode.Shared;

    private boolean lazyStartPartitionedProducers = false;

    private SortedMap<String, String> properties = new TreeMap<>();

    @ApiModelProperty(
            name = "initialSubscriptionName",
            value = "Use this configuration to automatically create an initial subscription when creating a topic."
                    + " If this field is not set, the initial subscription is not created."
    )
    private String initialSubscriptionName = null;

    /**
     *
     * Returns true if encryption keys are added.
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
            c.properties = new TreeMap<>(this.properties);
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
