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

import static org.testng.Assert.assertTrue;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.OpenClientHandles;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.awaitility.Awaitility;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Base class for tests that share a single {@link SharedPulsarCluster} singleton across all test classes.
 *
 * <p>Each test method gets its own unique namespace (created in {@link #setupSharedTest()} and
 * force-deleted in {@link #cleanupSharedTest()}), providing full isolation without the overhead
 * of starting a new broker per test.
 *
 * <p>Subclasses get access to {@link #admin} and {@link #pulsarClient} (initialized once per class)
 * and can use {@link #newTopicName()} to generate unique topic names within the test namespace.
 */
@CustomLog
public abstract class SharedPulsarBaseTest {

    private PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    private final List<String> namespaces = new ArrayList<>();
    private OpenClientHandles clientHandlesAtStart;

    /**
     * Whether the test methods must close the producers and consumers they create on {@link #pulsarClient}, which
     * outlives them: {@link #cleanupSharedTest} then fails a passed test method that leaves any of them open.
     */
    protected boolean checkClientHandlesClosed() {
        return false;
    }

    /**
     * Returns the unique namespace assigned to the current test method.
     */
    protected String getNamespace() {
        return namespaces.get(0);
    }

    /**
     * Returns the broker service URL (pulsar://...) for creating dedicated PulsarClient instances.
     */
    protected String getBrokerServiceUrl() {
        return pulsar.getBrokerServiceUrl();
    }

    /**
     * The shared {@link PulsarService}, for tests that need broker-internal accessors.
     */
    protected PulsarService getPulsar() {
        return pulsar;
    }

    /**
     * Returns the web service URL (http://...) for HTTP-based lookups and admin operations.
     */
    protected String getWebServiceUrl() {
        return pulsar.getWebServiceAddress();
    }

    /**
     * Creates a new PulsarClient connected to the shared cluster. The caller is responsible for closing it.
     */
    protected PulsarClient newPulsarClient() throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
    }

    /**
     * Returns the shared broker's {@link ServiceConfiguration} for runtime config inspection or changes.
     */
    protected ServiceConfiguration getConfig() {
        return pulsar.getConfig();
    }

    /**
     * Returns the shared broker's {@link SchemaStorage} instance.
     */
    protected SchemaStorage getSchemaStorage() {
        return pulsar.getSchemaStorage();
    }

    /**
     * Returns the topic reference for an already-loaded topic, if present.
     */
    protected Optional<Topic> getTopicReference(String topic) {
        return pulsar.getBrokerService().getTopicReference(topic);
    }

    /**
     * Looks up a topic by name, optionally creating it if it doesn't exist.
     */
    protected CompletableFuture<Optional<Topic>> getTopic(String topic, boolean createIfMissing) {
        return pulsar.getBrokerService().getTopic(topic, createIfMissing);
    }

    /**
     * Returns the topic if it exists in the broker, loading it from storage if necessary.
     */
    protected CompletableFuture<Optional<Topic>> getTopicIfExists(String topic) {
        return pulsar.getBrokerService().getTopicIfExists(topic);
    }

    /**
     * Initializes the shared cluster singleton and sets up {@link #admin} and {@link #pulsarClient}.
     * Called once per test class.
     */
    @BeforeClass(alwaysRun = true)
    public void setupSharedCluster() throws Exception {
        SharedPulsarCluster cluster = SharedPulsarCluster.get();
        pulsar = cluster.getPulsarService();
        admin = cluster.getAdmin();
        pulsarClient = cluster.getClient();
    }

    /**
     * Creates a unique namespace for the current test method. The namespace is automatically
     * cleaned up in {@link #cleanupSharedTest()}.
     */
    @BeforeMethod(alwaysRun = true)
    public void setupSharedTest() throws Exception {
        clientHandlesAtStart = OpenClientHandles.of(pulsarClient);
        createNewNamespace();
    }

    /**
     * Force-deletes all namespaces created during the test method, after checking that a passed test method closed
     * its producers and consumers when {@link #checkClientHandlesClosed()}.
     */
    @AfterMethod(alwaysRun = true)
    public void cleanupSharedTest(ITestResult result) throws Exception {
        try {
            if (checkClientHandlesClosed() && result.isSuccess()) {
                // Handles that earlier test classes left open can close meanwhile, so the counts may drop below
                // the ones at the start. No poll delay: Awaitility's default one would add 100 ms to every method
                Awaitility.await().pollDelay(Duration.ZERO).untilAsserted(() -> {
                    OpenClientHandles handles = OpenClientHandles.of(pulsarClient);
                    assertTrue(handles.producers() <= clientHandlesAtStart.producers()
                                    && handles.consumers() <= clientHandlesAtStart.consumers(),
                            result.getMethod().getMethodName() + " left producers or consumers of the shared client"
                                    + " open: " + clientHandlesAtStart + " at the start, " + handles + " at the end");
                });
            }
        } finally {
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
    }

    /**
     * Creates a new namespace under the shared tenant and registers it for automatic cleanup.
     */
    protected String createNewNamespace() throws Exception {
        String nsName = "test-" + UUID.randomUUID().toString().substring(0, 8);
        String ns = SharedPulsarCluster.TENANT_NAME + "/" + nsName;
        admin.namespaces().createNamespace(ns, Set.of(SharedPulsarCluster.CLUSTER_NAME));
        namespaces.add(ns);
        log.info().attr("testNamespace", ns).log("Created test namespace");
        return ns;
    }

    /**
     * Generates a unique persistent topic name within the current test namespace.
     */
    protected String newTopicName() {
        return "persistent://" + getNamespace() + "/topic-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
