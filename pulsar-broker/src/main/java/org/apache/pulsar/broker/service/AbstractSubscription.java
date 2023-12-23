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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.CommandSubscribe.IsolationLevel;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;

public abstract class AbstractSubscription implements Subscription {
    protected static final String SUBSCRIPTION_ISOLATION_LEVEL_PROPERTY = "pulsar.subscription.isolation.level";
    protected final LongAdder bytesOutFromRemovedConsumers = new LongAdder();
    protected final LongAdder msgOutFromRemovedConsumer = new LongAdder();

    public static void wrapIsolationLevelToProperties(Map<String, String> properties, IsolationLevel isolationLevel) {
        if (properties != null) {
            properties.put(SUBSCRIPTION_ISOLATION_LEVEL_PROPERTY, String.valueOf(isolationLevel.getValue()));
        }
    }

    public IsolationLevel fetchIsolationLevelFromProperties(Map<String, String> properties) {
        if (properties == null) {
            return IsolationLevel.READ_COMMITTED;
        }

        if (properties.containsKey(SUBSCRIPTION_ISOLATION_LEVEL_PROPERTY)) {
            IsolationLevel isolationLevel = IsolationLevel.valueOf(Integer.parseInt(properties.get(SUBSCRIPTION_ISOLATION_LEVEL_PROPERTY)));
            return isolationLevel != null ? isolationLevel : IsolationLevel.READ_COMMITTED;
        } else {
            return IsolationLevel.READ_COMMITTED;
        }
    }

    public long getMsgOutCounter() {
        return msgOutFromRemovedConsumer.longValue() + sumConsumers(Consumer::getMsgOutCounter);
    }

    public long getBytesOutCounter() {
        return bytesOutFromRemovedConsumers.longValue() + sumConsumers(Consumer::getBytesOutCounter);
    }

    private long sumConsumers(ToLongFunction<Consumer> toCounter) {
        return Optional.ofNullable(getDispatcher())
                .map(dispatcher -> dispatcher.getConsumers().stream().mapToLong(toCounter).sum())
                .orElse(0L);
    }
}
