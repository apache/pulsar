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
package org.apache.pulsar.common.policies.data;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;

/**
 * Policy value holder for different hierarchy level.
 * Currently, we have three hierarchy with priority : topic > namespace > broker.
 */
public class PolicyHierarchyValue<T> {
    private static final AtomicReferenceFieldUpdater<PolicyHierarchyValue, Object> VALUE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PolicyHierarchyValue.class, Object.class, "value");

    @Getter
    private volatile T brokerValue;

    @Getter
    private volatile T namespaceValue;

    @Getter
    private volatile T topicValue;

    private volatile T value;

    public PolicyHierarchyValue() {
    }

    public void updateBrokerValue(T brokerValue) {
        this.brokerValue = brokerValue;
        updateValue();
    }

    public void updateNamespaceValue(T namespaceValue) {
        this.namespaceValue = namespaceValue;
        updateValue();
    }

    public void updateTopicValue(T topicValue) {
        this.topicValue = topicValue;
        updateValue();
    }

    private void updateValue() {
        VALUE_UPDATER.updateAndGet(this, (preValue) -> {
            if (topicValue != null) {
                return topicValue;
            } else if (namespaceValue != null) {
                return namespaceValue;
            } else {
                return brokerValue;
            }
        });
    }

    public T get() {
        return value;
    }
}
