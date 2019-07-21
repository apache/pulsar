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
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Data;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProducerConfigurationData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private String topicName = null;

    private String producerName = null;
    private long sendTimeoutMs = 30000;
    private boolean blockIfQueueFull = false;
    private int maxPendingMessages = 1000;
    private int maxPendingMessagesAcrossPartitions = 50000;
    private MessageRoutingMode messageRoutingMode = null;
    private HashingScheme hashingScheme = HashingScheme.JavaStringHash;

    private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

    @JsonIgnore
    private MessageRouter customMessageRouter = null;

    private long batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1);
    private int batchingMaxMessages = 1000;
    private boolean batchingEnabled = true; // enabled by default
    @JsonIgnore
    private BatcherBuilder batcherBuilder = BatcherBuilder.DEFAULT;

    @JsonIgnore
    private CryptoKeyReader cryptoKeyReader;

    @JsonIgnore
    private Set<String> encryptionKeys = new TreeSet<>();

    private CompressionType compressionType = CompressionType.NONE;

    // Cannot use Optional<Long> since it's not serializable
    private Long initialSequenceId = null;

    private boolean autoUpdatePartitions = true;

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
}
