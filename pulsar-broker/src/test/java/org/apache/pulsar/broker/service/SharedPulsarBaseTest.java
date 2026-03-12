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

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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
@Slf4j
public abstract class SharedPulsarBaseTest {

    private PulsarService pulsar;
    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    private String namespace;

    /**
     * Returns the unique namespace assigned to the current test method.
     */
    protected String getNamespace() {
        return namespace;
    }

    /**
     * Returns the broker service URL (pulsar://...) for creating dedicated PulsarClient instances.
     */
    protected String getBrokerServiceUrl() {
        return pulsar.getBrokerServiceUrl();
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
        String nsName = "test-" + UUID.randomUUID().toString().substring(0, 8);
        String ns = SharedPulsarCluster.TENANT_NAME + "/" + nsName;
        namespace = ns;
        admin.namespaces().createNamespace(ns, Set.of(SharedPulsarCluster.CLUSTER_NAME));
        log.info("Created test namespace: {}", ns);
    }

    /**
     * Force-deletes the namespace created by {@link #setupSharedTest()}, including all topics in it.
     */
    @AfterMethod(alwaysRun = true)
    public void cleanupSharedTest() throws Exception {
        String ns = namespace;
        if (ns != null) {
            namespace = null;
            try {
                admin.namespaces().deleteNamespace(ns, true);
                log.info("Deleted test namespace: {}", ns);
            } catch (Exception e) {
                log.warn("Failed to delete namespace {}: {}", ns, e.getMessage());
            }
        }
    }

    /**
     * Generates a unique persistent topic name within the current test namespace.
     */
    protected String newTopicName() {
        return "persistent://" + namespace + "/topic-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
