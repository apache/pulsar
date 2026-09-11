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
package org.apache.pulsar.client.api.v5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.broker.service.SharedMultiBrokerPulsarBaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

/**
 * Base class for V5 client end-to-end tests that need a multi-broker cluster.
 *
 * <p>Companion to {@link V5ClientBaseTest} (single-broker). Use this when a test specifically
 * exercises broker-side coordination across brokers — controller-leader placement,
 * admin-operation redirects to the controller-owning broker, V5 consumer reconnection to a
 * different broker, etc. For non-multi-broker concerns prefer the single-broker variant: it's
 * faster and has fewer moving parts.
 *
 * <p>Builds one V5 {@link PulsarClient} per broker in the shared cluster (initialized in
 * {@code @BeforeClass}, closed in {@code @AfterClass}) so tests can drive operations through a
 * specific broker and observe how the system routes them. {@link #v5Client} aliases the first
 * client; {@link #v5Clients} exposes the full list aligned with {@link #brokers}.
 */
public abstract class V5MultiBrokerClientBaseTest extends SharedMultiBrokerPulsarBaseTest {

    /** V5 client connected to broker 0. Shorthand for {@code v5Clients.get(0)}. */
    protected PulsarClient v5Client;

    /** Per-broker V5 clients, in the same order as {@link #brokers}. */
    protected List<PulsarClient> v5Clients;

    private final List<PulsarClient> trackedClients = new ArrayList<>();

    @BeforeClass(alwaysRun = true)
    public void setupSharedV5MultiBrokerClients() throws Exception {
        List<PulsarClient> built = new ArrayList<>(brokers.size());
        for (var broker : brokers) {
            built.add(PulsarClient.builder()
                    .serviceUrl(broker.getBrokerServiceUrl())
                    .build());
        }
        v5Clients = Collections.unmodifiableList(built);
        v5Client = v5Clients.get(0);
    }

    @AfterClass(alwaysRun = true)
    public void closeSharedV5MultiBrokerClients() throws Exception {
        if (v5Clients != null) {
            for (PulsarClient c : v5Clients) {
                try {
                    c.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
            v5Clients = null;
            v5Client = null;
        }
    }

    /**
     * Build a fresh V5 client connected to the given broker. The returned client is
     * registered for automatic close at the end of the current test method — useful for
     * tests that want a dedicated client (e.g. to sever just its connection).
     */
    protected PulsarClient newV5Client(int brokerIndex) throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokers.get(brokerIndex).getBrokerServiceUrl())
                .build();
        trackedClients.add(client);
        return client;
    }

    @AfterMethod(alwaysRun = true)
    public void closeTrackedV5Clients() {
        for (int i = trackedClients.size() - 1; i >= 0; i--) {
            try {
                trackedClients.get(i).close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
        }
        trackedClients.clear();
    }

    /**
     * Create a scalable topic in the current test namespace and return its
     * {@code topic://...} name.
     */
    protected String newScalableTopic(int numInitialSegments) throws Exception {
        String name = "topic://" + getNamespace() + "/scalable-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(name, numInitialSegments);
        return name;
    }
}
