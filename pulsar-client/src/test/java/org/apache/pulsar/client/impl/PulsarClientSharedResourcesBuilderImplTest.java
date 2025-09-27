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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.testng.annotations.Test;

public class PulsarClientSharedResourcesBuilderImplTest {
    @Test
    public void testSharedResources() throws PulsarClientException {
        runClientsWithSharedResources(PulsarClientSharedResources.builder().build(), 1000);
    }

    private static void runClientsWithSharedResources(PulsarClientSharedResources sharedResources, int numberOfClients)
            throws PulsarClientException {
        List<PulsarClient> clients = new ArrayList<>();
        for (int i = 0; i < numberOfClients; i++) {
            clients.add(PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .sharedResources(sharedResources)
                    .build());
        }
        for (PulsarClient client : clients) {
            client.close();
        }
        sharedResources.close();
    }

    @Test
    public void testPulsarClientSharedResourcesBuilderUsingPublicAPI() throws PulsarClientException {
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                .configureEventLoop(eventLoopGroupConfig -> {
                    eventLoopGroupConfig
                            .name("testEventLoop")
                            .numberOfThreads(10);
                })
                .configureDnsResolver(dnsResolverConfig -> {
                    dnsResolverConfig.localAddress(new InetSocketAddress(0));
                })
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ListenerExecutor,
                        threadPoolConfig -> {
                            threadPoolConfig.name("testListenerThreadPool")
                                    .daemon(true)
                                    .numberOfThreads(12);
                        }
                )
                .configureThreadPool(PulsarClientSharedResources.SharedResource.InternalExecutor,
                        threadPoolConfig -> {
                            threadPoolConfig.name("testInternalThreadPool")
                                    .daemon(true)
                                    .numberOfThreads(2);
                        }
                )
                .configureThreadPool(PulsarClientSharedResources.SharedResource.LookupExecutor,
                        threadPoolConfig -> {
                            threadPoolConfig.name("testLookupThreadPool")
                                    .daemon(true)
                                    .numberOfThreads(1);
                        }
                )
                .configureThreadPool(PulsarClientSharedResources.SharedResource.ScheduledExecutor,
                        threadPoolConfig -> {
                            threadPoolConfig.name("testSchedulerThreadPool")
                                    .daemon(true)
                                    .numberOfThreads(1);
                        }
                )
                .configureTimer(timerConfig -> {
                    timerConfig.name("testTimer").tickDuration(100, TimeUnit.MILLISECONDS);
                })
                .build();
        runClientsWithSharedResources(sharedResources, 1000);
        sharedResources.close();
    }

    @Test
    public void testPartialSharing() throws PulsarClientException {
        PulsarClientSharedResources sharedResources =
                PulsarClientSharedResources.builder()
                        .shareConfigured()
                        .configureEventLoop(eventLoopGroupConfig -> {
                    eventLoopGroupConfig.name("testEventLoop").numberOfThreads(10);
                }).configureDnsResolver(dnsResolverConfig -> {
                    dnsResolverConfig.localAddress(new InetSocketAddress(0));
                }).build();
        runClientsWithSharedResources(sharedResources, 2);
        sharedResources.close();
    }

    @Test
    public void testDnsResolverConfig() throws PulsarClientException {
        PulsarClientSharedResources sharedResources =
                PulsarClientSharedResources.builder()
                        .shareConfigured()
                        .configureDnsResolver(dnsResolverConfig -> {
                            dnsResolverConfig
                                    .localAddress(new InetSocketAddress(0))
                                    .serverAddresses(List.of(new InetSocketAddress("8.8.8.8", 53)))
                                    .minTtl(0)
                                    .maxTtl(45)
                                    .negativeTtl(10)
                                    .ndots(1)
                                    .searchDomains(List.of("mycompany.com"))
                                    .queryTimeoutMillis(5000L)
                                    .tcpFallbackEnabled(true)
                                    .tcpFallbackOnTimeoutEnabled(false)
                                    .traceEnabled(false);
                        }).build();
        runClientsWithSharedResources(sharedResources, 2);
        sharedResources.close();
    }
}