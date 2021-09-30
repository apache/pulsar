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

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfigurationData<T> implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private Set<String> topicNames = Sets.newTreeSet();

    private Pattern topicsPattern;

    private String subscriptionName;

    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @JsonIgnore
    private MessageListener<T> messageListener;

    @JsonIgnore
    private ConsumerEventListener consumerEventListener;

    private int receiverQueueSize = 1000;

    private long acknowledgementsGroupTimeMicros = TimeUnit.MILLISECONDS.toMicros(100);

    private long negativeAckRedeliveryDelayMicros = TimeUnit.MINUTES.toMicros(1);

    private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

    private String consumerName = null;

    private long ackTimeoutMillis = 0;

    private long tickDurationMillis = 1000;

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

    // max pending chunked message to avoid sending incomplete message into the queue and memory
    private int maxPendingChunkedMessage = 10;

    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    private long expireTimeOfIncompleteChunkedMessageMillis = 60 * 1000;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader = null;

    @JsonIgnore
    private transient MessageCrypto messageCrypto = null;

    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private SortedMap<String, String> properties = new TreeMap<>();

    private boolean readCompacted = false;

    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    private int patternAutoDiscoveryPeriod = 60;

    private RegexSubscriptionMode regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;

    private transient DeadLetterPolicy deadLetterPolicy;

    private boolean retryEnable = false;

    @JsonIgnore
    private BatchReceivePolicy batchReceivePolicy;

    private boolean autoUpdatePartitions = true;

    private long autoUpdatePartitionsIntervalSeconds = 60;

    private boolean replicateSubscriptionState = false;

    private boolean resetIncludeHead = false;

    private transient KeySharedPolicy keySharedPolicy;

    private boolean batchIndexAckEnabled = false;

    private boolean ackReceiptEnabled = false;
    
    private boolean poolMessages = false;

    @JsonIgnore
    private transient MessagePayloadProcessor payloadProcessor = null;

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
            c.properties = Maps.newTreeMap(this.properties);
            return c;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ConsumerConfigurationData");
        }
    }
}
