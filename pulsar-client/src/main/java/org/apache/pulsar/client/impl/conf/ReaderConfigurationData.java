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

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.ReaderInterceptor;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;

@Data
public class ReaderConfigurationData<T> implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @ApiModelProperty(
            name = "topicNames",
            required = true,
            value = "Topic name"
    )
    private Set<String> topicNames = new HashSet<>();

    @JsonIgnore
    private MessageId startMessageId;

    @JsonIgnore
    private long startMessageFromRollbackDurationInSec;

    @ApiModelProperty(
            name = "receiverQueueSize",
            value = "Size of a consumer's receiver queue.\n"
                    + "\n"
                    + "For example, the number of messages that can be accumulated by a consumer before an "
                    + "application calls `Receive`.\n"
                    + "\n"
                    + "A value higher than the default value increases consumer throughput, though at the expense of "
                    + "more memory utilization."
    )
    private int receiverQueueSize = 1000;

    @ApiModelProperty(
            name = "readerListener",
            value = "A listener that is called for message received."
    )
    private ReaderListener<T> readerListener;

    @ApiModelProperty(
            name = "readerName",
            value = "Reader name"
    )
    private String readerName = null;

    @ApiModelProperty(
            name = "subscriptionRolePrefix",
            value = "Prefix of subscription role."
    )
    private String subscriptionRolePrefix = null;

    @ApiModelProperty(
            name = "subscriptionName",
            value = "Subscription name"
    )
    private String subscriptionName = null;

    @ApiModelProperty(
            name = "cryptoKeyReader",
            value = "Interface that abstracts the access to a key store."
    )
    private CryptoKeyReader cryptoKeyReader = null;

    @ApiModelProperty(
            name = "cryptoFailureAction",
            value = "Consumer should take action when it receives a message that can not be decrypted.\n"
                    + "* **FAIL**: this is the default option to fail messages until crypto succeeds.\n"
                    + "* **DISCARD**: silently acknowledge and not deliver message to an application.\n"
                    + "* **CONSUME**: deliver encrypted messages to applications. It is the application's"
                    + " responsibility to decrypt the message.\n"
                    + "\n"
                    + "The message decompression fails.\n"
                    + "\n"
                    + "If messages contain batch messages, a client is not be able to retrieve individual messages in"
                    + " batch.\n"
                    + "\n"
                    + "Delivered encrypted message contains {@link EncryptionContext} which contains encryption and "
                    + "compression information in it using which application can decrypt consumed message payload."
    )
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @ApiModelProperty(
            name = "readCompacted",
            value = "If enabling `readCompacted`, a consumer reads messages from a compacted topic rather than a full "
                    + "message backlog of a topic.\n"
                    + "\n"
                    + "A consumer only sees the latest value for each key in the compacted topic, up until reaching "
                    + "the point in the topic message when compacting backlog. Beyond that point, send messages as "
                    + "normal.\n"
                    + "\n"
                    + "`readCompacted` can only be enabled on subscriptions to persistent topics, which have a single "
                    + "active consumer (for example, failure or exclusive subscriptions).\n"
                    + "\n"
                    + "Attempting to enable it on subscriptions to non-persistent topics or on shared subscriptions "
                    + "leads to a subscription call throwing a `PulsarClientException`."
    )
    private boolean readCompacted = false;

    @ApiModelProperty(
            name = "resetIncludeHead",
            value = "If set to true, the first message to be returned is the one specified by `messageId`.\n"
                    + "\n"
                    + "If set to false, the first message to be returned is the one next to the message specified by "
                    + "`messageId`."
    )
    private boolean resetIncludeHead = false;

    private transient List<Range> keyHashRanges;

    private boolean poolMessages = false;

    private boolean autoUpdatePartitions = true;
    private long autoUpdatePartitionsIntervalSeconds = 60;

    private transient List<ReaderInterceptor<T>> readerInterceptorList;

    // max pending chunked message to avoid sending incomplete message into the queue and memory
    private int maxPendingChunkedMessage = 10;

    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    private long expireTimeOfIncompleteChunkedMessageMillis = TimeUnit.MINUTES.toMillis(1);

    private SubscriptionMode subscriptionMode = SubscriptionMode.NonDurable;

    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @JsonIgnore
    public String getTopicName() {
        if (topicNames.size() > 1) {
            throw new IllegalArgumentException("topicNames needs to be = 1");
        }
        return topicNames.iterator().next();
    }

    @JsonIgnore
    public void setTopicName(String topicNames) {
        //Compatible with a single topic
        this.topicNames.clear();
        this.topicNames.add(topicNames);
    }

    @SuppressWarnings("unchecked")
    public ReaderConfigurationData<T> clone() {
        try {
            ReaderConfigurationData<T> clone = (ReaderConfigurationData<T>) super.clone();
            clone.setTopicNames(new HashSet<>(clone.getTopicNames()));
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ReaderConfigurationData");
        }
    }
}
