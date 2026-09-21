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
package org.apache.pulsar.tests.performance.tools;

import org.apache.pulsar.client.api.PulsarClientSharedResources;

final class SharedClientResources {
    static PulsarClientSharedResources create(IotScenario scenario) {
        return PulsarClientSharedResources.builder()
                .configureEventLoop(config -> config.numberOfThreads(scenario.ioThreads()))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ListenerExecutor,
                        config -> config.numberOfThreads(scenario.listenerThreads()))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.InternalExecutor,
                        config -> config.numberOfThreads(scenario.ioThreads()))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ScheduledExecutor,
                        config -> config.numberOfThreads(Math.min(2, scenario.ioThreads())))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.LookupExecutor,
                        config -> config.numberOfThreads(Math.min(2, scenario.ioThreads())))
                .build();
    }

    private SharedClientResources() {
    }
}
