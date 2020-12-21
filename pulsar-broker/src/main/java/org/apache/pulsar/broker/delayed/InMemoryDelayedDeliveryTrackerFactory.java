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
package org.apache.pulsar.broker.delayed;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;

public class InMemoryDelayedDeliveryTrackerFactory implements DelayedDeliveryTrackerFactory {

    private Timer timer;

    private long tickTimeMillis;

    @Override
    public void initialize(ServiceConfiguration config) {
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-delayed-delivery"),
                config.getDelayedDeliveryTickTimeMillis(), TimeUnit.MILLISECONDS);
        this.tickTimeMillis = config.getDelayedDeliveryTickTimeMillis();
    }

    @Override
    public DelayedDeliveryTracker newTracker(PersistentDispatcherMultipleConsumers dispatcher) {
        return new InMemoryDelayedDeliveryTracker(dispatcher, timer, tickTimeMillis);
    }

    @Override
    public void close() {
        if (timer != null) {
            timer.stop();
        }
    }

}
