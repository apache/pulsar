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
package org.apache.pulsar.broker.admin;

import lombok.CustomLog;
import org.apache.avro.reflect.AvroAlias;
import org.apache.avro.reflect.AvroDefault;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-admin")
public class AdminApiSchemaAutoUpdateTest extends SharedPulsarBaseTest {

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @SuppressWarnings("deprecation")
    private void testAutoUpdateBackward(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                                                                    SchemaAutoUpdateCompatibilityStrategy.Backward);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with forward compat, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat schema should be rejected");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with backward compat, should succeed");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            p.send(new V2Data("test2"));
        }

    }

    @SuppressWarnings("deprecation")
    private void testAutoUpdateForward(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                                                                    SchemaAutoUpdateCompatibilityStrategy.Forward);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with backward compat, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat schema should be rejected");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with forward compat, should succeed");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            p.send(new V3Data("test2", 1, 2));
        }
    }

    @SuppressWarnings("deprecation")
    private void testAutoUpdateFull(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }

        log.info("try with backward compat only, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat only schema should fail");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with forward compat only, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat only schema should fail");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with fully compat");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            p.send(new V4Data("test2", 1, (short) 100));
        }
    }

    @SuppressWarnings("deprecation")
    private void testAutoUpdateDisabled(String namespace, String topicName) throws Exception {
        Assert.assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace));

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled);

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test1", 1));
        }
        log.info("try with backward compat only, should fail");
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
            Assert.fail("Backward compat only schema should fail");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with forward compat only, should fail");
        try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
            Assert.fail("Forward compat only schema should fail");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("try with fully compat, should fail");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            Assert.fail("Fully compat schema should fail, autoupdate disabled");
        } catch (PulsarClientException e) {
            Throwable actEx = FutureUtil.unwrapCompletionException(e);
            Assert.assertTrue(actEx instanceof PulsarClientException.IncompatibleSchemaException, actEx.getMessage());
        }

        log.info("Should still be able to connect with original schema");
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
            p.send(new V1Data("test2", 2));
        }

        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespace,
                SchemaAutoUpdateCompatibilityStrategy.Full);

        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespace),
                        SchemaAutoUpdateCompatibilityStrategy.Full));

        log.info("try with fully compat, again");
        try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
            p.send(new V4Data("test2", 1, (short) 100));
        }
    }

    @AvroAlias(space = "blah", alias = "data")
    static class V1Data {
        String foo;
        int bar;

        V1Data(String foo, int bar) {
            this.foo = foo;
            this.bar = bar;
        }
    }

    // backward compatible with V1Data
    @AvroAlias(space = "blah", alias = "data")
    static class V2Data {
        String foo;

        V2Data(String foo) {
            this.foo = foo;
        }
    }

    // forward compatible with V1Data
    @AvroAlias(space = "blah", alias = "data")
    static class V3Data {
        String foo;
        int bar;
        long baz;

        V3Data(String foo, int bar, long baz) {
            this.foo = foo;
            this.bar = bar;
            this.baz = baz;
        }
    }

    // fully compatible with V1Data
    @AvroAlias(space = "blah", alias = "data")
    static class V4Data {
        String foo;
        int bar;
        @AvroDefault(value = "10")
        short blah;

        V4Data(String foo, int bar, short blah) {
            this.foo = foo;
            this.bar = bar;
            this.blah = blah;
        }
    }

    @Test
    public void testBackwardV2() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateBackward(getNamespace(), "persistent://" + getNamespace() + "/backward");
        testAutoUpdateBackward(ns2, "non-persistent://" + ns2 + "/backward-np");
    }

    @Test
    public void testForwardV2() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateForward(getNamespace(), "persistent://" + getNamespace() + "/forward");
        testAutoUpdateForward(ns2, "non-persistent://" + ns2 + "/forward-np");
    }

    @Test
    public void testFullV2() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateFull(getNamespace(), "persistent://" + getNamespace() + "/full");
        testAutoUpdateFull(ns2, "non-persistent://" + ns2 + "/full-np");
    }

    @Test
    public void testDisabledV2() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateDisabled(getNamespace(), "persistent://" + getNamespace() + "/disabled");
        testAutoUpdateDisabled(ns2, "non-persistent://" + ns2 + "/disabled-np");
    }

    @Test
    public void testBackwardV1() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateBackward(getNamespace(), "persistent://" + getNamespace() + "/backward");
        testAutoUpdateBackward(ns2, "non-persistent://" + ns2 + "/backward-np");
    }

    @Test
    public void testForwardV1() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateForward(getNamespace(), "persistent://" + getNamespace() + "/forward");
        testAutoUpdateForward(ns2, "non-persistent://" + ns2 + "/forward-np");
    }

    @Test
    public void testFullV1() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateFull(getNamespace(), "persistent://" + getNamespace() + "/full");
        testAutoUpdateFull(ns2, "non-persistent://" + ns2 + "/full-np");
    }

    @Test
    public void testDisabledV1() throws Exception {
        String ns2 = createNewNamespace();
        testAutoUpdateDisabled(getNamespace(), "persistent://" + getNamespace() + "/disabled");
        testAutoUpdateDisabled(ns2, "non-persistent://" + ns2 + "/disabled-np");
    }

    @Test(timeOut = 60_000)
    @SuppressWarnings("deprecation")
    public void testIsAllowAutoUpdateSchemaWithReplicator() throws Exception {
        final String namespace = getNamespace();
        final String topic = newTopicName();
        admin.topics().createNonPartitionedTopic(topic);
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topic, false).join().get();

        // By default, it is true.
        Awaitility.await().untilAsserted(() -> {
            Assert.assertTrue(admin.namespaces().getPolicies(namespace).is_allow_auto_update_schema_with_replicator);
            Assert.assertTrue(persistentTopic.getIsAllowAutoUpdateSchemaWithReplicator());
        });

        try {
            admin.namespaces().setIsAllowAutoUpdateSchema(
                    namespace, true, false);
            Assert.fail("Should have thrown exception");
        } catch (PulsarAdminException e) {
            Assert.assertTrue(e.getMessage().contains("Can not enable for all producers but denies for replicators,"
                    + " which is meaningless"));
        }

        admin.namespaces().setIsAllowAutoUpdateSchema(
                namespace, false, false);
        Awaitility.await().untilAsserted(() -> {
            // namespace level.
            Policies policies = admin.namespaces().getPolicies(namespace);
            Assert.assertFalse(policies.is_allow_auto_update_schema);
            Assert.assertFalse(policies.is_allow_auto_update_schema_with_replicator);
            // topic level.
            Assert.assertFalse(persistentTopic.getIsAllowAutoUpdateSchemaWithReplicator());
        });

        admin.namespaces().setIsAllowAutoUpdateSchema(
                namespace, true, true);
        Awaitility.await().untilAsserted(() -> {
            // namespace level.
            Policies policies = admin.namespaces().getPolicies(namespace);
            Assert.assertTrue(policies.is_allow_auto_update_schema);
            Assert.assertTrue(policies.is_allow_auto_update_schema_with_replicator);
            // topic level.
            Assert.assertTrue(persistentTopic.getIsAllowAutoUpdateSchemaWithReplicator());
        });

        admin.namespaces().setIsAllowAutoUpdateSchema(
                namespace, false, true);
        Awaitility.await().untilAsserted(() -> {
            // namespace level.
            Policies policies = admin.namespaces().getPolicies(namespace);
            Assert.assertFalse(policies.is_allow_auto_update_schema);
            Assert.assertTrue(policies.is_allow_auto_update_schema_with_replicator);
            // topic level.
            Assert.assertTrue(persistentTopic.getIsAllowAutoUpdateSchemaWithReplicator());
        });

        // cleanup.
        admin.topics().delete(topic);
    }
}
