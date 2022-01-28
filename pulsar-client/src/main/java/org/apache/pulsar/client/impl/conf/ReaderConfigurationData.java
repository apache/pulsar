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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Data;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.ReaderListener;

@Data
public class ReaderConfigurationData<T> implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private Set<String> topicNames = new HashSet<>();

    @JsonIgnore
    private MessageId startMessageId;

    @JsonIgnore
    private long startMessageFromRollbackDurationInSec;

    private int receiverQueueSize = 1000;

    private ReaderListener<T> readerListener;

    private String readerName = null;
    private String subscriptionRolePrefix = null;
    private String subscriptionName = null;

    private CryptoKeyReader cryptoKeyReader = null;
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    private boolean readCompacted = false;
    private boolean resetIncludeHead = false;

    private transient List<Range> keyHashRanges;

    private boolean poolMessages = false;

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
