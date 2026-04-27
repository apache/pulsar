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
import org.testng.annotations.AfterMethod;

/**
 * Base class for V5 client end-to-end tests.
 *
 * <p>Extends {@link SharedPulsarBaseTest} (one shared in-memory broker per JVM, fresh
 * namespace per test method) and adds:
 * <ul>
 *   <li>{@link #newV5Client()} — V5 PulsarClient against the shared broker.</li>
 *   <li>{@link #newScalableTopic(int)} — creates a {@code topic://...} scalable topic with the
 *       given number of initial segments and returns its name.</li>
 *   <li>Auto-tracking of clients/producers/consumers created during a test method, closed in
 *       {@link #closeV5Resources()} after the test.</li>
 * </ul>
 */
public abstract class V5ClientBaseTest extends SharedPulsarBaseTest {

    private final List<AutoCloseable> v5Resources = new ArrayList<>();

    /**
     * Build a fresh V5 PulsarClient connected to the shared cluster's binary service URL.
     * The returned client is registered for automatic close in {@link #closeV5Resources()}.
     */
    protected PulsarClient newV5Client() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .build();
        track(client);
        return client;
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
     */
    protected <T extends AutoCloseable> T track(T closeable) {
        v5Resources.add(closeable);
        return closeable;
    }

    @AfterMethod(alwaysRun = true)
    public void closeV5Resources() {
        // Close in reverse order: consumers/producers before the client they belong to.
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
