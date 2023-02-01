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

package org.apache.pulsar.broker.testcontext;

import lombok.Value;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.mockito.Mockito;
import org.mockito.internal.creation.instance.ConstructorInstantiator;

@lombok.Builder(builderClassName = "Builder", toBuilder = true)
@Value
public class SpyConfig {
    public enum SpyType {
        NONE,
        SPY,
        SPY_ALSO_INVOCATIONS;

        public <T> T spy(T object) {
            if (object == null) {
                return null;
            }
            switch (this) {
                case NONE:
                    return object;
                case SPY:
                    return BrokerTestUtil.spyWithoutRecordingInvocations(object);
                case SPY_ALSO_INVOCATIONS:
                    return Mockito.spy(object);
                default:
                    throw new UnsupportedOperationException("Unknown spy type: " + this);
            }
        }

        public <T> T spy(Class<T> clazz, Object... args) {
            switch (this) {
                case NONE:
                    // Use Mockito's internal class to instantiate the object
                    return new ConstructorInstantiator(false, args).newInstance(clazz);
                case SPY:
                    return BrokerTestUtil.spyWithClassAndConstructorArgs(clazz, args);
                case SPY_ALSO_INVOCATIONS:
                    return BrokerTestUtil.spyWithClassAndConstructorArgsRecordingInvocations(clazz, args);
                default:
                    throw new UnsupportedOperationException("Unknown spy type: " + this);
            }
        }
    }

    private final SpyType pulsarBroker;
    private final SpyType pulsarResources;
    private final SpyType brokerService;
    private final SpyType bookKeeperClient;

    public static Builder builder() {
        return builder(SpyType.NONE);
    }

    public static Builder builder(SpyType defaultSpyType) {
        Builder spyConfigBuilder = new Builder();
        spyConfigBuilder.pulsarBroker(defaultSpyType);
        spyConfigBuilder.pulsarResources(defaultSpyType);
        spyConfigBuilder.brokerService(defaultSpyType);
        spyConfigBuilder.bookKeeperClient(defaultSpyType);
        return spyConfigBuilder;
    }
}
