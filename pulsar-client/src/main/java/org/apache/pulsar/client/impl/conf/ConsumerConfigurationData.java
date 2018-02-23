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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.SubscriptionType;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;

import lombok.Data;

@Data
public class ConsumerConfigurationData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<String> topicNames = Sets.newTreeSet();

    private String subscriptionName;

    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @JsonIgnore
    private MessageListener messageListener;

    @JsonIgnore
    private ConsumerEventListener consumerEventListener;

    private int receiverQueueSize = 1000;

    private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

    private String consumerName = null;

    private long ackTimeoutMillis = 0;

    private int priorityLevel = 0;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader = null;

    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private final Map<String, String> properties = new TreeMap<>();

    private boolean readCompacted = false;

    @JsonIgnore
    public String getSingleTopic() {
        checkArgument(topicNames.size() == 1);
        return topicNames.iterator().next();
    }

}
