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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.MessageListenerExecutor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfigurationData<T> implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(
            name = "topicNames",
            required = true,
            value = "Topic name"
    )
    private Set<String> topicNames = new TreeSet<>();

    @ApiModelProperty(
            name = "topicsPattern",
            value = "The regexp for the topic name(not contains partition suffix)."
    )
    private Pattern topicsPattern;

    @ApiModelProperty(
            name = "subscriptionName",
            value = "Subscription name"
    )
    private String subscriptionName;

    @ApiModelProperty(
            name = "subscriptionType",
            value = "Subscription type.\n"
                    + "Four subscription types are available:\n"
                    + "* Exclusive\n"
                    + "* Failover\n"
                    + "* Shared\n"
                    + "* Key_Shared"
    )
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private Map<String, String> subscriptionProperties;

    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @JsonIgnore
    private MessageListenerExecutor messageListenerExecutor;
    @JsonIgnore
    private MessageListener<T> messageListener;

    @JsonIgnore
    private ConsumerEventListener consumerEventListener;

    @ApiModelProperty(
            name = "negativeAckRedeliveryBackoff",
            value = "Interface for custom message is negativeAcked policy. You can specify `RedeliveryBackoff` for a"
                    + " consumer."
    )
    @JsonIgnore
    private RedeliveryBackoff negativeAckRedeliveryBackoff;

    @ApiModelProperty(
            name = "ackTimeoutRedeliveryBackoff",
            value = "Interface for custom message is ackTimeout policy. You can specify `RedeliveryBackoff` for a"
                    + " consumer."
    )
    @JsonIgnore
    private RedeliveryBackoff ackTimeoutRedeliveryBackoff;

    @ApiModelProperty(
            name = "receiverQueueSize",
            value = "Size of a consumer's receiver queue.\n"
                    + "\n"
                    + "For example, the number of messages accumulated by a consumer before an application calls "
                    + "`Receive`.\n"
                    + "\n"
                    + "A value higher than the default value increases consumer throughput, though at the expense of "
                    + "more memory utilization."
    )
    private int receiverQueueSize = 1000;

    @ApiModelProperty(
            name = "acknowledgementsGroupTimeMicros",
            value = "Group a consumer acknowledgment for a specified time.\n"
                    + "\n"
                    + "By default, a consumer uses 100ms grouping time to send out acknowledgments to a broker.\n"
                    + "\n"
                    + "Setting a group time of 0 sends out acknowledgments immediately.\n"
                    + "\n"
                    + "A longer ack group time is more efficient at the expense of a slight increase in message "
                    + "re-deliveries after a failure."
    )
    private long acknowledgementsGroupTimeMicros = TimeUnit.MILLISECONDS.toMicros(100);

    @ApiModelProperty(
            name = "maxAcknowledgmentGroupSize",
            value = "Group a consumer acknowledgment for the number of messages."
    )
    private int maxAcknowledgmentGroupSize = 1000;

    @ApiModelProperty(
            name = "negativeAckRedeliveryDelayMicros",
            value = "Delay to wait before redelivering messages that failed to be processed.\n"
                    + "\n"
                    + "When an application uses {@link Consumer#negativeAcknowledge(Message)}, failed messages are "
                    + "redelivered after a fixed timeout."
    )
    private long negativeAckRedeliveryDelayMicros = TimeUnit.MINUTES.toMicros(1);

    @ApiModelProperty(
            name = "maxTotalReceiverQueueSizeAcrossPartitions",
            value = "The max total receiver queue size across partitions.\n"
                    + "\n"
                    + "This setting reduces the receiver queue size for individual partitions if the total receiver "
                    + "queue size exceeds this value."
    )
    private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

    @ApiModelProperty(
            name = "consumerName",
            value = "Consumer name"
    )
    private String consumerName = null;

    @ApiModelProperty(
            name = "ackTimeoutMillis",
            value = "Timeout of unacked messages"
    )
    private long ackTimeoutMillis = 0;

    @ApiModelProperty(
            name = "tickDurationMillis",
            value = "Granularity of the ack-timeout redelivery.\n"
                    + "\n"
                    + "Using an higher `tickDurationMillis` reduces the memory overhead to track messages when setting "
                    + "ack-timeout to a bigger value (for example, 1 hour)."
    )
    private long tickDurationMillis = 1000;

    @ApiModelProperty(
            name = "priorityLevel",
            value = "Priority level for a consumer to which a broker gives more priority while dispatching messages "
                    + "in Shared subscription type.\n"
                    + "\n"
                    + "The broker follows descending priorities. For example, 0=max-priority, 1, 2,...\n"
                    + "\n"
                    + "In Shared subscription type, the broker **first dispatches messages to the max priority level "
                    + "consumers if they have permits**. Otherwise, the broker considers next priority level consumers."
                    + "\n\n"
                    + "**Example 1**\n"
                    + "If a subscription has consumerA with `priorityLevel` 0 and consumerB with `priorityLevel` 1,"
                    + " then the broker **only dispatches messages to consumerA until it runs out permits** and then"
                    + " starts dispatching messages to consumerB.\n"
                    + "\n"
                    + "**Example 2**\n"
                    + "Consumer Priority, Level, Permits\n"
                    + "C1, 0, 2\n"
                    + "C2, 0, 1\n"
                    + "C3, 0, 1\n"
                    + "C4, 1, 2\n"
                    + "C5, 1, 1\n"
                    + "\n"
                    + "Order in which a broker dispatches messages to consumers is: C1, C2, C3, C1, C4, C5, C4."
    )
    private int priorityLevel = 0;

    /**
     * @deprecated use {@link #setMaxPendingChunkedMessage(int)}
     */
    @Deprecated
    public void setMaxPendingChuckedMessage(int maxPendingChuckedMessage) {
        this.maxPendingChunkedMessage = maxPendingChuckedMessage;
    }

    /**
     * @deprecated use {@link #getMaxPendingChunkedMessage()}
     */
    @Deprecated
    public int getMaxPendingChuckedMessage() {
        return maxPendingChunkedMessage;
    }

    @ApiModelProperty(
            name = "maxPendingChunkedMessage",
            value = "The maximum size of a queue holding pending chunked messages. When the threshold is reached,"
                    + " the consumer drops pending messages to optimize memory utilization."
    )
    // max pending chunked message to avoid sending incomplete message into the queue and memory
    private int maxPendingChunkedMessage = 10;

    @ApiModelProperty(
            name = "autoAckOldestChunkedMessageOnQueueFull",
            value = "Whether to automatically acknowledge pending chunked messages when the threshold of"
                    + " `maxPendingChunkedMessage` is reached. If set to `false`, these messages will be redelivered"
                    + " by their broker."
    )
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @ApiModelProperty(
            name = "expireTimeOfIncompleteChunkedMessageMillis",
            value = "The time interval to expire incomplete chunks if a consumer fails to receive all the chunks in the"
                    + " specified time period. The default value is 1 minute."
    )
    private long expireTimeOfIncompleteChunkedMessageMillis = TimeUnit.MINUTES.toMillis(1);

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader = null;

    @JsonIgnore
    private transient MessageCrypto messageCrypto = null;

    @ApiModelProperty(
            name = "cryptoFailureAction",
            value = "Consumer should take action when it receives a message that can not be decrypted.\n"
                    + "* **FAIL**: this is the default option to fail messages until crypto succeeds.\n"
                    + "* **DISCARD**:silently acknowledge and not deliver message to an application.\n"
                    + "* **CONSUME**: deliver encrypted messages to applications. It is the application's"
                    + " responsibility to decrypt the message.\n"
                    + "\n"
                    + "The decompression of message fails.\n"
                    + "\n"
                    + "If messages contain batch messages, a client is not be able to retrieve individual messages in"
                    + " batch.\n"
                    + "\n"
                    + "Delivered encrypted message contains {@link EncryptionContext} which contains encryption and "
                    + "compression information in it using which application can decrypt consumed message payload."
    )
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @ApiModelProperty(
            name = "properties",
            value = "A name or value property of this consumer.\n"
                    + "\n"
                    + "`properties` is application defined metadata attached to a consumer.\n"
                    + "\n"
                    + "When getting a topic stats, associate this metadata with the consumer stats for easier "
                    + "identification."
    )
    private SortedMap<String, String> properties = new TreeMap<>();

    @ApiModelProperty(
            name = "readCompacted",
            value = "If enabling `readCompacted`, a consumer reads messages from a compacted topic rather than reading "
                    + "a full message backlog of a topic.\n"
                    + "\n"
                    + "A consumer only sees the latest value for each key in the compacted topic, up until reaching "
                    + "the point in the topic message when compacting backlog. Beyond that point, send messages as "
                    + "normal.\n"
                    + "\n"
                    + "Only enabling `readCompacted` on subscriptions to persistent topics, which have a single active "
                    + "consumer (like failure or exclusive subscriptions).\n"
                    + "\n"
                    + "Attempting to enable it on subscriptions to non-persistent topics or on shared subscriptions "
                    + "leads to a subscription call throwing a `PulsarClientException`."
    )
    private boolean readCompacted = false;

    @ApiModelProperty(
            name = "subscriptionInitialPosition",
            value = "Initial position at which to set cursor when subscribing to a topic at first time."
    )
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @ApiModelProperty(
            name = "patternAutoDiscoveryPeriod",
            value = "Topic auto discovery period when using a pattern for topic's consumer.\n"
                    + "\n"
                    + "The default value is 1 minute, with a minimum of 1 second."
    )
    private int patternAutoDiscoveryPeriod = 60;

    @ApiModelProperty(
            name = "regexSubscriptionMode",
            value = "When subscribing to a topic using a regular expression, you can pick a certain type of topics.\n"
                    + "\n"
                    + "* **PersistentOnly**: only subscribe to persistent topics.\n"
                    + "* **NonPersistentOnly**: only subscribe to non-persistent topics.\n"
                    + "* **AllTopics**: subscribe to both persistent and non-persistent topics."
    )
    private RegexSubscriptionMode regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;

    @ApiModelProperty(
            name = "deadLetterPolicy",
            value = "Dead letter policy for consumers.\n"
                    + "\n"
                    + "By default, some messages are probably redelivered many times, even to the extent that it "
                    + "never stops.\n"
                    + "\n"
                    + "By using the dead letter mechanism, messages have the max redelivery count. **When exceeding the"
                    + " maximum number of redeliveries, messages are sent to the Dead Letter Topic and acknowledged "
                    + "automatically**.\n"
                    + "\n"
                    + "You can enable the dead letter mechanism by setting `deadLetterPolicy`.\n"
                    + "\n"
                    + "**Example**\n"
                    + "```java\n"
                    + "client.newConsumer()\n"
                    + ".deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())\n"
                    + ".subscribe();\n"
                    + "```\n"
                    + "Default dead letter topic name is `{TopicName}-{Subscription}-DLQ`.\n"
                    + "\n"
                    + "To set a custom dead letter topic name:\n"
                    + "```java\n"
                    + "client.newConsumer()\n"
                    + ".deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10)\n"
                    + ".deadLetterTopic(\"your-topic-name\").build())\n"
                    + ".subscribe();\n"
                    + "```\n"
                    + "When specifying the dead letter policy while not specifying `ackTimeoutMillis`, you can set the"
                    + " ack timeout to 30000 millisecond."
    )
    private transient DeadLetterPolicy deadLetterPolicy;

    private boolean retryEnable = false;

    @JsonIgnore
    private BatchReceivePolicy batchReceivePolicy;

    @ApiModelProperty(
            name = "autoUpdatePartitions",
            value = "If `autoUpdatePartitions` is enabled, a consumer subscribes to partition increasement "
                    + "automatically.\n"
                    + "\n"
                    + "**Note**: this is only for partitioned consumers."
    )
    private boolean autoUpdatePartitions = true;

    private long autoUpdatePartitionsIntervalSeconds = 60;

    @ApiModelProperty(
            name = "replicateSubscriptionState",
            value = "If `replicateSubscriptionState` is enabled, a subscription state is replicated to geo-replicated"
                    + " clusters."
    )
    private boolean replicateSubscriptionState = false;

    private boolean resetIncludeHead = false;

    @JsonIgnore
    private transient KeySharedPolicy keySharedPolicy;

    private boolean batchIndexAckEnabled = false;

    private boolean ackReceiptEnabled = false;

    private boolean poolMessages = false;

    @JsonIgnore
    private transient MessagePayloadProcessor payloadProcessor = null;

    private boolean startPaused = false;

    private boolean autoScaledReceiverQueueSizeEnabled = false;

    private List<TopicConsumerConfigurationData> topicConfigurations = new ArrayList<>();

    public TopicConsumerConfigurationData getMatchingTopicConfiguration(String topicName) {
        return topicConfigurations.stream()
                .filter(topicConf -> topicConf.getTopicNameMatcher().matches(topicName))
                .findFirst()
                .orElseGet(() -> TopicConsumerConfigurationData.ofTopicName(topicName, this));
    }

    public void setTopicConfigurations(List<TopicConsumerConfigurationData> topicConfigurations) {
        checkArgument(topicConfigurations != null, "topicConfigurations should not be null.");
        this.topicConfigurations = topicConfigurations;
    }

    public void setAutoUpdatePartitionsIntervalSeconds(int interval, TimeUnit timeUnit) {
        checkArgument(interval > 0, "interval needs to be > 0");
        this.autoUpdatePartitionsIntervalSeconds = timeUnit.toSeconds(interval);
    }

    @JsonIgnore
    public String getSingleTopic() {
        checkArgument(topicNames.size() == 1, "topicNames needs to be = 1");
        return topicNames.iterator().next();
    }

    public ConsumerConfigurationData<T> clone() {
        try {
            @SuppressWarnings("unchecked")
            ConsumerConfigurationData<T> c = (ConsumerConfigurationData<T>) super.clone();
            c.topicNames = Sets.newTreeSet(this.topicNames);
            c.properties = new TreeMap<>(this.properties);
            return c;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ConsumerConfigurationData");
        }
    }
}
