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
package org.apache.pulsar.client.impl;

import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;

@Data
@NoArgsConstructor
public class TableViewConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private String topicName = null;
    private String subscriptionName = null;
    private long autoUpdatePartitionsSeconds = 60;
    private String topicCompactionStrategyClassName = null;

    private CryptoKeyReader cryptoKeyReader = null;
    private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

    @Override
    public TableViewConfigurationData clone() {
        try {
            TableViewConfigurationData clone = (TableViewConfigurationData) super.clone();
            clone.setTopicName(topicName);
            clone.setAutoUpdatePartitionsSeconds(autoUpdatePartitionsSeconds);
            clone.setSubscriptionName(subscriptionName);
            clone.setTopicCompactionStrategyClassName(topicCompactionStrategyClassName);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
