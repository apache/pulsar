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
import org.apache.pulsar.broker.PulsarService;
import org.mockito.Mockito;
import org.mockito.internal.creation.instance.ConstructorInstantiator;

/**
 * Configuration for what kind of Mockito spy to use on collaborator objects
 * of the PulsarService managed by {@link PulsarTestContext}.
 *
 * This configuration is applied using {@link PulsarTestContext.Builder#spyConfig(SpyConfig)} or by
 * calling {@link PulsarTestContext.Builder#spyByDefault()} to enable spying for all configurable collaborator
 * objects.
 *
 * For verifying interactions with Mockito's {@link Mockito#verify(Object)} method, the spy type must be set to
 * {@link SpyType#SPY_ALSO_INVOCATIONS}. This is because the default spy type {@link SpyType#SPY}
 * does not record invocations.
 */
@lombok.Builder(builderClassName = "Builder", toBuilder = true)
@Value
public class SpyConfig {
    /**
     * Type of spy to use.
     */
    public enum SpyType {
        NONE, // No spy
        SPY, // Spy without recording invocations
        SPY_ALSO_INVOCATIONS; // Spy and record invocations

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

    /**
     * Spy configuration for {@link PulsarTestContext#getPulsarService()}.
     */
    private final SpyType pulsarService;
    /**
     * Spy configuration for {@link PulsarService#getPulsarResources()}.
     */
    private final SpyType pulsarResources;
    /**
     * Spy configuration for {@link PulsarService#getBrokerService()}.
     */
    private final SpyType brokerService;
    /**
     * Spy configuration for {@link PulsarService#getBookKeeperClient()}.
     * In the test context, this is the same as {@link PulsarTestContext#getBookKeeperClient()} .
     * It is a client that wraps an in-memory mock bookkeeper implementation.
     */
    private final SpyType bookKeeperClient;
    /**
     * Spy configuration for {@link PulsarService#getCompactor()}.
     */
    private final SpyType compactor;
    /**
     * Spy configuration for {@link PulsarService#getNamespaceService()}.
     */
    private final SpyType namespaceService;

    /**
     * Create a builder for SpyConfig with no spies by default.
     *
     * @return a builder
     */
    public static Builder builder() {
        return builder(SpyType.NONE);
    }

    /**
     * Create a builder for SpyConfig with the given spy type for all configurable collaborator objects.
     *
     * @param defaultSpyType the spy type to use for all configurable collaborator objects
     * @return a builder
     */
    public static Builder builder(SpyType defaultSpyType) {
        Builder spyConfigBuilder = new Builder();
        spyConfigBuilder.pulsarService(defaultSpyType);
        spyConfigBuilder.pulsarResources(defaultSpyType);
        spyConfigBuilder.brokerService(defaultSpyType);
        spyConfigBuilder.bookKeeperClient(defaultSpyType);
        spyConfigBuilder.compactor(defaultSpyType);
        spyConfigBuilder.namespaceService(defaultSpyType);
        return spyConfigBuilder;
    }
}
