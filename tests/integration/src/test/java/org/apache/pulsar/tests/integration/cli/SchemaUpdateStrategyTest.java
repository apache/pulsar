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
package org.apache.pulsar.tests.integration.cli;


import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.reflect.AvroAlias;
import org.apache.avro.reflect.AvroDefault;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test setting the schema update strategy via the CLI.
 */
public class SchemaUpdateStrategyTest extends PulsarTestSuite {
    private static final Logger log = LoggerFactory.getLogger(SchemaUpdateStrategyTest.class);

    private void testAutoUpdateBackward(String namespace, String topicName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-schema-autoupdate-strategy", namespace);
        Assert.assertEquals(result.getStdout().trim(), "FULL");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-schema-autoupdate-strategy",
                "--compatibility", "BACKWARD", namespace);

        try (PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build()) {
            V1Data v1Data = new V1Data("test1", 1);
            try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
                p.send(v1Data);
            }

            log.info("try with forward compat, should fail");
            try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
                Assert.fail("Forward compat schema should be rejected");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with backward compat, should succeed");
            V2Data v2Data = new V2Data("test2");
            try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
                p.send(v2Data);
            }

            Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(schema)
                 .topic(topicName)
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscriptionName("sub")
                 .subscribe()
            ) {
                log.info("Schema Info : {}", schema.getSchemaInfo().getSchemaDefinition());

                Message<GenericRecord> msg1 = consumer.receive();
                v1Data.assertEqualToRecord(msg1.getValue());

                Message<GenericRecord> msg2 = consumer.receive();
                v2Data.assertEqualToRecord(msg2.getValue());
            }
        }
    }

    private void testNone(String namespace, String topicName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-schema-autoupdate-strategy", namespace);
        Assert.assertEquals(result.getStdout().trim(), "FULL");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-schema-autoupdate-strategy",
                "--compatibility", "NONE", namespace);

        try (PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build()) {
            V1Data v1Data = new V1Data("test1", 1);
            try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
                p.send(v1Data);
            }

            log.info("try with forward compat, should succeed");
            V3Data v3Data = new V3Data("test3", 1, 2);
            try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
                p.send(v3Data);
            }

            log.info("try with backward compat, should succeed");
            V2Data v2Data = new V2Data("test2");
            try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
                p.send(v2Data);
            }

            Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(schema)
                 .topic(topicName)
                 .subscriptionName("sub")
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscribe()
            ) {
                log.info("Schema Info : {}", schema.getSchemaInfo().getSchemaDefinition());

                Message<GenericRecord> msg1 = consumer.receive();
                v1Data.assertEqualToRecord(msg1.getValue());

                Message<GenericRecord> msg2 = consumer.receive();
                v3Data.assertEqualToRecord(msg2.getValue());

                Message<GenericRecord> msg3 = consumer.receive();
                v2Data.assertEqualToRecord(msg3.getValue());
            }
        }
    }

    private void testAutoUpdateForward(String namespace, String topicName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-schema-autoupdate-strategy", namespace);
        Assert.assertEquals(result.getStdout().trim(), "FULL");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-schema-autoupdate-strategy",
                                                 "--compatibility", "FORWARD", namespace);

        try (PulsarClient pulsarClient = PulsarClient.builder()
             .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build()) {

            V1Data v1Data = new V1Data("test1", 1);
            try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
                p.send(v1Data);
            }

            log.info("try with backward compat, should fail");
            try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
                Assert.fail("Backward compat schema should be rejected");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with forward compat, should succeed");
            V3Data v3Data = new V3Data("test2", 1, 2);
            try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
                p.send(v3Data);
            }

            Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(schema)
                 .topic(topicName)
                 .subscriptionName("sub")
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscribe()
            ) {
                log.info("Schema Info : {}", schema.getSchemaInfo().getSchemaDefinition());

                Message<GenericRecord> msg1 = consumer.receive();
                v1Data.assertEqualToRecord(msg1.getValue());

                Message<GenericRecord> msg2 = consumer.receive();
                v3Data.assertEqualToRecord(msg2.getValue());
            }
        }

    }

    private void testAutoUpdateFull(String namespace, String topicName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-schema-autoupdate-strategy", namespace);
        Assert.assertEquals(result.getStdout().trim(), "FULL");

        try (PulsarClient pulsarClient = PulsarClient.builder()
             .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build()) {

            V1Data v1Data = new V1Data("test1", 1);
            try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
                p.send(v1Data);
            }

            log.info("try with backward compat only, should fail");
            try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
                Assert.fail("Backward compat only schema should fail");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with forward compat only, should fail");
            try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
                Assert.fail("Forward compat only schema should fail");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with fully compat");
            V4Data v4Data = new V4Data("test2", 1, (short) 100);
            try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
                p.send(v4Data);
            }

            Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
            try (Consumer<GenericRecord> consumer = pulsarClient.newConsumer(schema)
                 .topic(topicName)
                 .subscriptionName("sub")
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscribe()
            ) {
                log.info("Schema Info : {}", schema.getSchemaInfo().getSchemaDefinition());

                Message<GenericRecord> msg1 = consumer.receive();
                v1Data.assertEqualToRecord(msg1.getValue());

                Message<GenericRecord> msg2 = consumer.receive();
                v4Data.assertEqualToRecord(msg2.getValue());
            }
        }
    }

    private void testAutoUpdateDisabled(String namespace, String topicName) throws Exception {
        ContainerExecResult result = pulsarCluster.runAdminCommandOnAnyBroker(
                "namespaces", "get-schema-autoupdate-strategy", namespace);
        Assert.assertEquals(result.getStdout().trim(), "FULL");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "set-schema-autoupdate-strategy",
                                                 "--disabled", namespace);

        try (PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build()) {
            try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topicName).create()) {
                p.send(new V1Data("test1", 1));
            }
            log.info("try with backward compat only, should fail");
            try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class)).topic(topicName).create()) {
                Assert.fail("Backward compat only schema should fail");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with forward compat only, should fail");
            try (Producer<V3Data> p = pulsarClient.newProducer(Schema.AVRO(V3Data.class)).topic(topicName).create()) {
                Assert.fail("Forward compat only schema should fail");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("try with fully compat, should fail");
            try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class)).topic(topicName).create()) {
                Assert.fail("Fully compat schema should fail, autoupdate disabled");
            } catch (PulsarClientException e) {
                Assert.assertTrue(e.getMessage().contains("IncompatibleSchemaException"));
            }

            log.info("Manually set new schema");
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> schema = new HashMap<>();
            schema.put("type", "AVRO");
            schema.put("schema", Schema.AVRO(V4Data.class).getSchemaInfo().getSchemaDefinition());
            BrokerContainer b = pulsarCluster.getAnyBroker();
            String schemaFile = String.format("/tmp/schema-%s", UUID.randomUUID().toString());
            b.putFile(schemaFile, mapper.writeValueAsBytes(schema));

            b.execCmd(PulsarCluster.ADMIN_SCRIPT, "schemas", "upload", "-f", schemaFile, topicName);

            boolean success = false;
            for (int i = 0; i < 50; i++) {
                try (Producer<V4Data> p = pulsarClient.newProducer(Schema.AVRO(V4Data.class))
                        .topic(topicName).create()) {
                    p.send(new V4Data("test2", 1, (short)100));
                    success = true;
                    break;
                } catch (Throwable t) {
                    // expected a few times until the broker sees the new schema
                }
                Thread.sleep(100);
            }
            Assert.assertTrue(success, "Should have been able to use new schema");
        }
    }

    @AvroAlias(space="blah", alias="data")
    static class V1Data {
        String foo;
        int bar;

        V1Data(String foo, int bar) {
            this.foo = foo;
            this.bar = bar;
        }

        void assertEqualToRecord(GenericRecord record) {
            assertEquals(
                2, record.getFields().size(),
                record.getFields().size() + " fields in found : " + record.getFields());
            assertEquals(foo, record.getField("foo"));
            assertEquals(Integer.valueOf(bar), record.getField("bar"));
        }
    }

    // backward compatible with V1Data
    @AvroAlias(space="blah", alias="data")
    static class V2Data {
        String foo;

        V2Data(String foo) {
            this.foo = foo;
        }

        void assertEqualToRecord(GenericRecord record) {
            assertEquals(
                1, record.getFields().size(),
                record.getFields().size() + " fields in found : " + record.getFields());
            assertEquals(foo, record.getField("foo"));
        }
    }

    // forward compatible with V1Data
    @AvroAlias(space="blah", alias="data")
    static class V3Data {
        String foo;
        int bar;
        long baz;

        V3Data(String foo, int bar, long baz) {
            this.foo = foo;
            this.bar = bar;
            this.baz = baz;
        }

        void assertEqualToRecord(GenericRecord record) {
            assertEquals(
                3, record.getFields().size(),
                record.getFields().size() + " fields in found : " + record.getFields());
            assertEquals(foo, record.getField("foo"));
            assertEquals(Integer.valueOf(bar), record.getField("bar"));
            assertEquals(Long.valueOf(baz), record.getField("baz"));
        }
    }

    // fully compatible with V1Data
    @AvroAlias(space="blah", alias="data")
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

        void assertEqualToRecord(GenericRecord record) {
            assertEquals(
                3, record.getFields().size(),
                record.getFields().size() + " fields in found : " + record.getFields());
            assertEquals(foo, record.getField("foo"));
            assertEquals(Integer.valueOf(bar), record.getField("bar"));
            // NOTE: in generic record, avro returns integer. we can consider improving the
            // the behavior in future to reflect the right java class.
            assertEquals(Integer.valueOf(blah), record.getField("blah"));
        }
    }

    @Test
    public void testBackwardV2() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/bw-p-v2");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/bw-np-v2");

        testAutoUpdateBackward("public/bw-p-v2", "persistent://public/bw-p-v2/topic1");
        testAutoUpdateBackward("public/bw-np-v2", "non-persistent://public/bw-np-v2/topic1");
    }

    @Test
    public void testForwardV2() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/fw-p-v2");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/fw-np-v2");

        testAutoUpdateForward("public/fw-p-v2", "persistent://public/fw-p-v2/topic1");
        testAutoUpdateForward("public/fw-np-v2", "non-persistent://public/fw-np-v2/topic1");
    }

    @Test
    public void testFullV2() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/full-p-v2");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/full-np-v2");

        testAutoUpdateFull("public/full-p-v2", "persistent://public/full-p-v2/topic1");
        testAutoUpdateFull("public/full-np-v2", "non-persistent://public/full-np-v2/topic1");
    }

    @Test
    public void testNoneV2() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), "public/none-p-v2");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                pulsarCluster.getClusterName(), "public/none-np-v2");

        testNone("public/none-p-v2", "persistent://public/none-p-v2/topic1");
        testNone("public/none-np-v2", "non-persistent://public/none-np-v2/topic1");
    }

    @Test
    public void testDisabledV2() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/dis-p-v2");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create", "-c",
                                                 pulsarCluster.getClusterName(), "public/dis-np-v2");

        testAutoUpdateDisabled("public/dis-p-v2", "persistent://public/dis-p-v2/topic1");
        testAutoUpdateDisabled("public/dis-np-v2", "non-persistent://public/dis-np-v2/topic1");
    }

    @Test
    public void testBackwardV1() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/b-p-v1");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/b-np-v1");
        testAutoUpdateBackward("public/" + pulsarCluster.getClusterName() + "/b-p-v1",
                               "persistent://public/" + pulsarCluster.getClusterName() + "/b-p-v1/topic1");
        testAutoUpdateBackward("public/" + pulsarCluster.getClusterName() + "/b-np-v1",
                               "persistent://public/" + pulsarCluster.getClusterName() + "/b-np-v1/topic1");
    }

    @Test
    public void testForwardV1() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/f-p-v1");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/f-np-v1");
        testAutoUpdateForward("public/" + pulsarCluster.getClusterName() + "/f-p-v1",
                              "persistent://public/" + pulsarCluster.getClusterName() + "/f-p-v1/topic1");
        testAutoUpdateForward("public/" + pulsarCluster.getClusterName() + "/f-np-v1",
                              "persistent://public/" + pulsarCluster.getClusterName() + "/f-np-v1/topic1");
    }

    @Test
    public void testFullV1() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/full-p-v1");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/full-np-v1");
        testAutoUpdateFull("public/" + pulsarCluster.getClusterName() + "/full-p-v1",
                           "persistent://public/" + pulsarCluster.getClusterName() + "/full-p-v1/topic1");
        testAutoUpdateFull("public/" + pulsarCluster.getClusterName() + "/full-np-v1",
                           "persistent://public/" + pulsarCluster.getClusterName() + "/full-np-v1/topic1");
    }

    @Test
    public void testNoneV1() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                "public/" + pulsarCluster.getClusterName() + "/none-p-v1");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                "public/" + pulsarCluster.getClusterName() + "/none-np-v1");
        testNone("public/" + pulsarCluster.getClusterName() + "/none-p-v1",
                "persistent://public/" + pulsarCluster.getClusterName() + "/none-p-v1/topic1");
        testNone("public/" + pulsarCluster.getClusterName() + "/none-np-v1",
                "persistent://public/" + pulsarCluster.getClusterName() + "/none-np-v1/topic1");
    }

    @Test
    public void testDisabledV1() throws Exception {
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/dis-p-v1");
        pulsarCluster.runAdminCommandOnAnyBroker("namespaces", "create",
                                                 "public/" + pulsarCluster.getClusterName() + "/dis-np-v1");
        testAutoUpdateDisabled("public/" + pulsarCluster.getClusterName() + "/dis-p-v1",
                               "persistent://public/" + pulsarCluster.getClusterName() + "/dis-p-v1/topic1");
        testAutoUpdateDisabled("public/" + pulsarCluster.getClusterName() + "/dis-np-v1",
                               "persistent://public/" + pulsarCluster.getClusterName() + "/dis-np-v1/topic1");
    }
}
