/**
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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.BundlesData;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class BrokerServiceBundlesCacheInvalidationTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRecreateNamespace() throws Exception {
        String namespace = "prop/test-" + System.nanoTime();
        String topic = namespace + "/my-topic";

        // First create namespace with 20 bundles
        admin.namespaces().createNamespace(namespace, 20);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.send("Hello");
        producer.close();

        // Delete and recreate with 32 bundles
        admin.topics().delete(topic);
        admin.namespaces().deleteNamespace(namespace, false);
        admin.namespaces().createNamespace(namespace, 32);

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        log.info("BUNDLES: {}", admin.namespaces().getBundles(namespace));
        assertEquals(bundlesData.getNumBundles(), 32);
    }
}