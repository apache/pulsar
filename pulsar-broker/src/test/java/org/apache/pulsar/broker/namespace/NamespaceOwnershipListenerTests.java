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
package org.apache.pulsar.broker.namespace;

import com.google.common.collect.Sets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
public class NamespaceOwnershipListenerTests extends BrokerTestBase {

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
    public void testNamespaceBundleOwnershipListener() throws Exception {

        final CountDownLatch countDownLatch = new CountDownLatch(2);
        final AtomicBoolean onLoad = new AtomicBoolean(false);
        final AtomicBoolean unLoad = new AtomicBoolean(false);

        final String namespace = "prop/" + UUID.randomUUID().toString();

        pulsar.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.getNamespaceObject().toString().equals(namespace);
            }

            @Override
            public void onLoad(NamespaceBundle bundle) {
                countDownLatch.countDown();
                onLoad.set(true);
            }

            @Override
            public void unLoad(NamespaceBundle bundle) {
                countDownLatch.countDown();
                unLoad.set(true);
            }
        });

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        assertTrue(admin.namespaces().getNamespaces("prop").contains(namespace));

        final String topic = "persistent://" + namespace + "/os-0";

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        producer.close();

        admin.namespaces().unload(namespace);

        countDownLatch.await();

        Assert.assertTrue(onLoad.get());
        Assert.assertTrue(unLoad.get());
        admin.topics().delete(topic);
        admin.namespaces().deleteNamespace(namespace);
    }

    @Test
    public void testGetAllPartitions() throws PulsarAdminException, ExecutionException, InterruptedException {
        final String namespace = "prop/" + UUID.randomUUID().toString();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        assertTrue(admin.namespaces().getNamespaces("prop").contains(namespace));

        final String topicName = "persistent://" + namespace + "/os";
        admin.topics().createPartitionedTopic(topicName, 6);

        List<String> partitions = pulsar.getNamespaceService().getAllPartitions(NamespaceName.get(namespace)).get();

        Assert.assertEquals(partitions.size(), 6);

        for (int i = 0; i < partitions.size(); i++) {
            Assert.assertEquals(partitions.get(i), topicName + "-partition-" + i);
        }

        admin.topics().deletePartitionedTopic(topicName);
        admin.namespaces().deleteNamespace(namespace);
    }

    @Test
    public void testNamespaceBundleLookupOnwershipListener() throws PulsarAdminException, InterruptedException,
            PulsarClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        final AtomicInteger onLoad = new AtomicInteger(0);
        final AtomicInteger unLoad = new AtomicInteger(0);

        final String namespace = "prop/" + UUID.randomUUID().toString();

        pulsar.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle bundle) {
                countDownLatch.countDown();
                onLoad.addAndGet(1);
            }

            @Override
            public void unLoad(NamespaceBundle bundle) {
                countDownLatch.countDown();
                unLoad.addAndGet(1);
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return namespaceBundle.getNamespaceObject().toString().equals(namespace);
            }
        });

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        assertTrue(admin.namespaces().getNamespaces("prop").contains(namespace));

        final String topic = "persistent://" + namespace + "/os-0";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        producer.close();

        admin.lookups().lookupTopic(topic);

        admin.namespaces().unload(namespace);

        countDownLatch.await();

        Assert.assertEquals(onLoad.get(), 1);
        Assert.assertEquals(unLoad.get(), 1);
        admin.topics().delete(topic);
        admin.namespaces().deleteNamespace(namespace);
    }
}
