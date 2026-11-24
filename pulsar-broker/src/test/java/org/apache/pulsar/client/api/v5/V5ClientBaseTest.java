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
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

/**
 * Base class for V5 client end-to-end tests.
 *
 * <p>Extends {@link SharedPulsarBaseTest} (one shared in-memory broker per JVM, fresh
 * namespace per test method) and adds:
 * <ul>
 *   <li>{@link #v5Client} — a shared V5 PulsarClient initialized once per test class
 *       (mirrors the v4 {@code pulsarClient} field on the parent). Most tests should
 *       just use this.</li>
 *   <li>{@link #newV5Client()} — for the rare case where a test needs its own
 *       dedicated client (e.g., to exercise client-lifecycle behavior). Tracked for
 *       cleanup automatically.</li>
 *   <li>{@link #newScalableTopic(int)} — creates a {@code topic://...} scalable topic
 *       with the given number of initial segments and returns its name.</li>
 * </ul>
 *
 * <p>Tests should prefer Lombok's {@code @Cleanup} on local producer / consumer
 * variables for resource lifecycle. {@link #track(AutoCloseable)} is available for
 * cases where {@code @Cleanup} doesn't fit (e.g., resources stored in fields).
 */
public abstract class V5ClientBaseTest extends SharedPulsarBaseTest {

    /** Shared V5 client. Initialized in {@code @BeforeClass}, closed in {@code @AfterClass}. */
    protected PulsarClient v5Client;

    private final List<AutoCloseable> v5Resources = new ArrayList<>();

    @BeforeClass(alwaysRun = true)
    public void setupSharedV5Client() throws Exception {
        v5Client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void closeSharedV5Client() throws Exception {
        if (v5Client != null) {
            v5Client.close();
            v5Client = null;
        }
    }

    /**
     * Build a fresh V5 PulsarClient connected to the shared cluster. The returned
     * client is registered for automatic close after the test method.
     *
     * <p>Most tests should use the shared {@link #v5Client} instead — only reach for this
     * when the test specifically needs an isolated client.
     */
    protected PulsarClient newV5Client() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .build();
        return track(client);
    }

    /**
     * Create a scalable topic with the given number of initial segments under the current
     * test namespace and return its fully-qualified {@code topic://...} name.
     */
    protected String newScalableTopic(int numInitialSegments) throws Exception {
        String name = "topic://" + getNamespace() + "/scalable-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(name, numInitialSegments);
        return name;
    }

    /**
     * Register an arbitrary {@link AutoCloseable} for automatic close after the test.
     * Prefer {@code @Cleanup} on local variables when possible.
     */
    protected <T extends AutoCloseable> T track(T closeable) {
        v5Resources.add(closeable);
        return closeable;
    }

    @AfterMethod(alwaysRun = true)
    public void closeV5Resources() {
        // Close in reverse order: nested resources before the things they hang off of.
        for (int i = v5Resources.size() - 1; i >= 0; i--) {
            AutoCloseable c = v5Resources.get(i);
            try {
                c.close();
            } catch (Exception ignored) {
                // Best-effort cleanup; tests have already asserted what they care about.
            }
        }
        v5Resources.clear();
    }
}
