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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Base class for tests that need a shared multi-broker cluster across test classes.
 *
 * <p>Companion to {@link SharedPulsarBaseTest}. Use this when a test specifically depends on
 * behavior that only manifests across brokers — namespace ownership transfer, controller-leader
 * failover, segment placement on different brokers, V5 client reconnect to a different broker.
 * For everything else, prefer the single-broker {@link SharedPulsarBaseTest}: it's faster, has
 * fewer moving parts, and is sufficient for most coverage.
 *
 * <p>Each test method gets a fresh namespace under {@link SharedMultiBrokerPulsarCluster#TENANT_NAME}
 * (created in {@link #setupSharedMultiBrokerTest()} and force-deleted in
 * {@link #cleanupSharedMultiBrokerTest()}). The cluster itself is JVM-wide and reused across
 * every test class that extends this — see {@link SharedMultiBrokerPulsarCluster}.
 *
 * <p>Subclasses get:
 * <ul>
 *   <li>{@link #admin} / {@link #pulsarClient} — handles aimed at broker 0; lookups against any
 *       broker correctly redirect to topic owners, so most tests should just use these.</li>
 *   <li>{@link #brokers} / {@link #admins} / {@link #clients} — full per-broker lists, in start
 *       order, for tests that need to address a specific broker (e.g. asserting topic
 *       ownership, killing a specific broker).</li>
 *   <li>{@link #newTopicName()} — generates a unique topic in the test namespace.</li>
 * </ul>
 */
@CustomLog
public abstract class SharedMultiBrokerPulsarBaseTest {

    /** All brokers in the shared cluster, in start order. */
    protected List<PulsarService> brokers;
    /** Per-broker admin handles, aligned with {@link #brokers}. */
    protected List<PulsarAdmin> admins;
    /** Per-broker client handles, aligned with {@link #brokers}. */
    protected List<PulsarClient> clients;

    /** Convenience: broker 0's admin. */
    protected PulsarAdmin admin;
    /** Convenience: broker 0's client. */
    protected PulsarClient pulsarClient;

    private final List<String> namespaces = new ArrayList<>();

    /** Returns the unique namespace assigned to the current test method. */
    protected String getNamespace() {
        return namespaces.get(0);
    }

    /** Returns the broker service URL for broker {@code index}. */
    protected String getBrokerServiceUrl(int index) {
        return brokers.get(index).getBrokerServiceUrl();
    }

    /** Returns the web service URL for broker {@code index}. */
    protected String getWebServiceUrl(int index) {
        return brokers.get(index).getWebServiceAddress();
    }

    /**
     * Creates a new {@link PulsarClient} connected to broker {@code index}. Callers are
     * responsible for closing the returned client.
     */
    protected PulsarClient newPulsarClient(int index) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(brokers.get(index).getBrokerServiceUrl()).build();
    }

    /**
     * Initializes (lazily) the shared cluster singleton and wires the per-class fields. Called
     * once per test class.
     */
    @BeforeClass(alwaysRun = true)
    public void setupSharedMultiBrokerCluster() throws Exception {
        SharedMultiBrokerPulsarCluster cluster = SharedMultiBrokerPulsarCluster.get();
        brokers = cluster.getBrokers();
        admins = cluster.getAdmins();
        clients = cluster.getClients();
        admin = cluster.getAdmin();
        pulsarClient = cluster.getClient();
    }

    /** Creates a unique namespace for the current test method. */
    @BeforeMethod(alwaysRun = true)
    public void setupSharedMultiBrokerTest() throws Exception {
        createNewNamespace();
    }

    /** Force-deletes all namespaces created during the test method. */
    @AfterMethod(alwaysRun = true)
    public void cleanupSharedMultiBrokerTest() throws Exception {
        for (String ns : namespaces) {
            try {
                admin.namespaces().deleteNamespace(ns, true);
                log.info().attr("testNamespace", ns).log("Deleted test namespace");
            } catch (Exception e) {
                log.warn().attr("deleteNamespace", ns).exceptionMessage(e).log("Failed to delete namespace");
            }
        }
        namespaces.clear();
    }

    /** Creates a new namespace under the shared tenant and registers it for cleanup. */
    protected String createNewNamespace() throws Exception {
        String nsName = "test-" + UUID.randomUUID().toString().substring(0, 8);
        String ns = SharedMultiBrokerPulsarCluster.TENANT_NAME + "/" + nsName;
        admin.namespaces().createNamespace(ns, Set.of(SharedMultiBrokerPulsarCluster.CLUSTER_NAME));
        namespaces.add(ns);
        log.info().attr("testNamespace", ns).log("Created test namespace");
        return ns;
    }

    /** Generates a unique persistent topic name within the current test namespace. */
    protected String newTopicName() {
        return "persistent://" + getNamespace() + "/topic-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
