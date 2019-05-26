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
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerConfigurationData<T> implements Serializable, Cloneable {
    @Builder.Default private static final long serialVersionUID = 1L;

    @Builder.Default private Set<String> topicNames = Sets.newTreeSet();

    private Pattern topicsPattern;

    private String subscriptionName;

    @Builder.Default private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @JsonIgnore
    private MessageListener<T> messageListener;

    @JsonIgnore
    private ConsumerEventListener consumerEventListener;

    @Builder.Default private int receiverQueueSize = 1000;

    @Builder.Default private long acknowledgementsGroupTimeMicros = TimeUnit.MILLISECONDS.toMicros(100);

    @Builder.Default private long negativeAckRedeliveryDelayMicros = TimeUnit.MINUTES.toMicros(1);

    @Builder.Default private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

    @Builder.Default private String consumerName = null;

    @Builder.Default private long ackTimeoutMillis = 0;

    @Builder.Default private long tickDurationMillis = 1000;

    @Builder.Default private int priorityLevel = 0;

    @JsonIgnore
    @Builder.Default private CryptoKeyReader cryptoKeyReader = null;

    @Builder.Default private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @Builder.Default private SortedMap<String, String> properties = new TreeMap<>();

    @Builder.Default private boolean readCompacted = false;

    @Builder.Default private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Builder.Default private int patternAutoDiscoveryPeriod = 1;

    @Builder.Default private RegexSubscriptionMode regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;

    @Builder.Default private DeadLetterPolicy deadLetterPolicy;

    @Builder.Default private boolean autoUpdatePartitions = true;

    private boolean replicateSubscriptionState = false;

    @JsonIgnore
    public String getSingleTopic() {
        checkArgument(topicNames.size() == 1);
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
