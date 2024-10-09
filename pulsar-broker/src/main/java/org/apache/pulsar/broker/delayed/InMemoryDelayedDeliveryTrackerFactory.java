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
package org.apache.pulsar.broker.delayed;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryDelayedDeliveryTrackerFactory implements DelayedDeliveryTrackerFactory {
    private static final Logger log = LoggerFactory.getLogger(InMemoryDelayedDeliveryTrackerFactory.class);

    private Timer timer;

    private long tickTimeMillis;

    private boolean isDelayedDeliveryDeliverAtTimeStrict;

    private long fixedDelayDetectionLookahead;

    @Override
    public void initialize(PulsarService pulsarService) {
        ServiceConfiguration config = pulsarService.getConfig();
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-delayed-delivery"),
                config.getDelayedDeliveryTickTimeMillis(), TimeUnit.MILLISECONDS);
        this.tickTimeMillis = config.getDelayedDeliveryTickTimeMillis();
        this.isDelayedDeliveryDeliverAtTimeStrict = config.isDelayedDeliveryDeliverAtTimeStrict();
        this.fixedDelayDetectionLookahead = config.getDelayedDeliveryFixedDelayDetectionLookahead();
    }

    @Override
    public DelayedDeliveryTracker newTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = dispatcher.getTopic().getName();
        String subscriptionName = dispatcher.getSubscription().getName();
        DelayedDeliveryTracker tracker =  DelayedDeliveryTracker.DISABLE;
        try {
            tracker = newTracker0(dispatcher);
        } catch (Exception e) {
            // it should never go here
            log.warn("Failed to create InMemoryDelayedDeliveryTracker, topic {}, subscription {}",
                    topicName, subscriptionName, e);
        }
        return tracker;
    }

    @VisibleForTesting
    InMemoryDelayedDeliveryTracker newTracker0(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        return new InMemoryDelayedDeliveryTracker(dispatcher, timer, tickTimeMillis,
                isDelayedDeliveryDeliverAtTimeStrict, fixedDelayDetectionLookahead);
    }

    @Override
    public void close() {
        if (timer != null) {
            timer.stop();
        }
    }

}
