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
package org.apache.pulsar.tests.integration.io.sources;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.policies.data.SourceStatusUtil;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.awaitility.Awaitility;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SourceStatus;

import static org.testng.Assert.*;

/**
 * A tester for testing kafka source with Avro/Protobuf Messages.
 * This test starts a PulsarCluster, a container with a Kafka Broker
 * and a container with the SchemaRegistry.
 * It populates a Kafka topic with Avro/Protobuf encoded messages with schema
 * and then it verifies that the records are correclty received
 * but a Pulsar Consumer
 */
@Slf4j
public class KafkaSourceTest extends PulsarFunctionsTestBase {
    public static final String CONFLUENT_PLATFORM_VERSION = System.getProperty("confluent.version", "6.2.8");

    private static final String SOURCE_TYPE = "kafka";

    private final String kafkaAvroTopicName = "kafkaavrosourcetopic";
    private final String kafkaProtoTopicName = "kafkaprotosourcetopic";

    private EnhancedKafkaContainer kafkaContainer;
    private SchemaRegistryContainer schemaRegistryContainer;

    protected final Map<String, Object> sourceConfig;
    protected final String kafkaContainerName = "kafkacontainer";
    protected final String schemaRegistryContainerName = "schemaregistry";

    public KafkaSourceTest() {
        sourceConfig = new HashMap<>();
    }

    @Test(groups = "source")
    public void testAvro() throws Exception {
        startKafkaContainers(pulsarCluster);
        try {
            sourceConfig.put("valueDeserializationClass", KafkaAvroDeserializer.class.getName());
            sourceConfig.put("topic", kafkaProtoTopicName);
            testSource("avro");
        } finally {
            stopKafkaContainers();
        }
    }

    @Test(groups = "source")
    public void testProtobuf() throws Exception {
        startKafkaContainers(pulsarCluster);
        try {
            sourceConfig.put("valueDeserializationClass", KafkaProtobufDeserializer.class.getName());
            sourceConfig.put("topic", kafkaAvroTopicName);
            testSource("proto");
        } finally {
            stopKafkaContainers();
        }
    }

    private String getBootstrapServersOnDockerNetwork() {
        return kafkaContainerName + ":9093";
    }


    public void startKafkaContainers(PulsarCluster cluster) throws Exception {
        this.kafkaContainer = createKafkaContainer(cluster);
        cluster.startService(kafkaContainerName, kafkaContainer);
        log.info("creating schema registry kafka {}",  getBootstrapServersOnDockerNetwork());
        this.schemaRegistryContainer = new SchemaRegistryContainer(getBootstrapServersOnDockerNetwork());
        cluster.startService(schemaRegistryContainerName, schemaRegistryContainer);
        sourceConfig.put("bootstrapServers", getBootstrapServersOnDockerNetwork());
        sourceConfig.put("groupId", "test-source-group");
        sourceConfig.put("fetchMinBytes", 1L);
        sourceConfig.put("autoCommitIntervalMs", 10L);
        sourceConfig.put("sessionTimeoutMs", 10000L);
        sourceConfig.put("heartbeatIntervalMs", 5000L);
        sourceConfig.put("consumerConfigProperties",
                ImmutableMap.of("schema.registry.url", getRegistryAddressInDockerNetwork())
        );
    }

    private class EnhancedKafkaContainer extends KafkaContainer {

        public EnhancedKafkaContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public String getBootstrapServers() {
            // we have to override this function
            // because we want the Kafka Broker to advertise itself
            // with the docker network address
            // otherwise the Kafka Schema Registry won't work
            return "PLAINTEXT://" + kafkaContainerName + ":9093";
        }

    }

    protected EnhancedKafkaContainer createKafkaContainer(PulsarCluster cluster) {
        return (EnhancedKafkaContainer) new EnhancedKafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
                .withEmbeddedZookeeper()
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                        .withName(kafkaContainerName)
                );
    }

    public void stopKafkaContainers() {
        if (null != schemaRegistryContainer) {
            PulsarCluster.stopService(schemaRegistryContainerName, schemaRegistryContainer);
        }
        if (null != kafkaContainer) {
            PulsarCluster.stopService(kafkaContainerName, kafkaContainer);
        }
    }

    public void prepareSource(String kafkaTopic) throws Exception {
        log.info("creating topic");
        ExecResult execResult = kafkaContainer.execInContainer(
            "/usr/bin/kafka-topics",
            "--create",
            "--zookeeper",
            getZooKeeperAddressInDockerNetwork(),
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--topic",
            kafkaTopic);
        assertTrue(
            execResult.getStdout().contains("Created topic"),
            execResult.getStdout());

    }

    private String getZooKeeperAddressInDockerNetwork() {
        return kafkaContainerName +":2181";
    }

    private void testSource(String type)  throws Exception {
        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "test-source-connector-"
                + functionRuntimeType + "-output-topic-" + type + randomName(8);
        final String sourceName = "test-source-connector-"
                + functionRuntimeType + "-name-" + type + randomName(8);
        final int numMessages = 10;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        Consumer<GenericRecord> consumer = client.newConsumer(Schema.AUTO_CONSUME())
                .topic(outputTopicName)
                .subscriptionName("sourcetester")
                .subscribe();

        String kafkaTopic = kafkaAvroTopicName;
        if (type.equals("proto")) {
            kafkaTopic = kafkaProtoTopicName;
        }

        // prepare the testing environment for source
        prepareSource(kafkaTopic);

        // submit the source connector
        submitSourceConnector(tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(tenant, namespace, sourceName);

        // get source status
        Awaitility.with()
                .timeout(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(10))
                .until(() -> {
                    try {
                        getSourceStatus(tenant, namespace, sourceName);
                        return true;
                    } catch (Throwable ex) {
                        log.error("Error while getting source status, will retry", ex);
                        return false;
                    }
                });

        if (type.equals("avro")) {
            testAvroSource(numMessages, kafkaTopic, tenant, namespace, sourceName, consumer);
        } else {
            testProtoSource(numMessages, kafkaTopic, tenant, namespace, sourceName, consumer);
        }

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    private void testAvroSource(int numMessages, String kafkaTopic, String tenant, String namespace, String sourceName, Consumer<GenericRecord> consumer) throws Exception {
        // produce messages
        List<MyBean> messages = produceAvroMessages(numMessages, kafkaTopic);

        // wait for source to process messages
        Awaitility.with()
                .timeout(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(10))
                .until(() -> {
                    try {
                        waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages);
                        return true;
                    } catch (Throwable ex) {
                        log.error("Error while processing source messages, will retry", ex);
                        return false;
                    }
                });

        // validate the source result
        validateSourceResultAvro(consumer, messages);
    }

    private void testProtoSource(int numMessages, String kafkaTopic, String tenant, String namespace, String sourceName, Consumer<GenericRecord> consumer) throws Exception {
        // produce messages
        produceProtoMessages(numMessages, kafkaTopic);

        // wait for source to process messages
        Awaitility.with()
                .timeout(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(10))
                .until(() -> {
                    try {
                        waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages);
                        return true;
                    } catch (Throwable ex) {
                        log.error("Error while processing source messages, will retry", ex);
                        return false;
                    }
                });

        // validate the source result
        validateSourceResultProto(consumer, numMessages);
    }

    public void validateSourceResultAvro(Consumer<GenericRecord> consumer,
                                         List<MyBean> beans) throws Exception {
        int recordsNumber = 0;
        Message<GenericRecord> msg = consumer.receive(10, TimeUnit.SECONDS);
        while (msg != null) {
            GenericRecord valueRecord = msg.getValue();
            Assert.assertNotNull(valueRecord.getFields());
            Assert.assertTrue(valueRecord.getFields().size() > 0);
            for (Field field : valueRecord.getFields()) {
                log.info("field {} value {}", field, valueRecord.getField(field));
            }
            assertEquals(beans.get(recordsNumber).field, valueRecord.getField("field"));
            consumer.acknowledge(msg);
            recordsNumber++;
            msg = consumer.receive(10, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, beans.size());
        log.info("Stop {} server container. topic: {} has {} records.", kafkaContainerName, consumer.getTopic(), recordsNumber);
    }

    public void validateSourceResultProto(Consumer<GenericRecord> consumer,
                                          int numMessages) throws Exception {
        int recordsNumber = 0;
        Message<GenericRecord> msg = consumer.receive(numMessages, TimeUnit.SECONDS);
        while (msg != null) {
            GenericRecord valueRecord = msg.getValue();
            Assert.assertNotNull(valueRecord.getFields());
            Assert.assertTrue(valueRecord.getFields().size() > 0);
            for (Field field : valueRecord.getFields()) {
                log.info("field {} value {}", field, valueRecord.getField(field));
            }
            assertEquals("pulsar_"+recordsNumber, valueRecord.getField("name"));
            assertEquals(recordsNumber, valueRecord.getField("number"));
            consumer.acknowledge(msg);
            recordsNumber++;
            msg = consumer.receive(10, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, numMessages);
        log.info("Stop {} server container. topic: {} has {} records.", kafkaContainerName, consumer.getTopic(), recordsNumber);
    }

    protected void getSourceInfoSuccess(String tenant,
                                        String namespace,
                                        String sourceName) throws Exception {
        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source",
                "get",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName
        };

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source info : {}", result.getStdout());
        assertTrue(
                result.getStdout().contains("\"archive\": \"builtin://" + SOURCE_TYPE + "\""),
                result.getStdout()
        );
    }

    protected void getSourceStatus(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source",
                "status",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        final SourceStatus sourceStatus = SourceStatusUtil.decode(result.getStdout());

        assertEquals(sourceStatus.getNumInstances(), 1);
        assertEquals(sourceStatus.getNumRunning(), 1);
        assertEquals(sourceStatus.getInstances().size(), 1);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);

        assertTrue(result.getStdout().contains("\"running\" : true"));

    }

    protected void submitSourceConnector(String tenant,
                                         String namespace,
                                         String sourceName,
                                         String outputTopicName) throws Exception {
        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source", "create",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName,
                "--source-type", SOURCE_TYPE,
                "--sourceConfig", new Gson().toJson(sourceConfig),
                "--destinationTopicName", outputTopicName
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("Created successfully"),
                result.getStdout());
    }

    @Data
    public static final class MyBean {
        private String field;
    }

    public List<MyBean> produceAvroMessages(int numMessages, String topic) throws Exception{
        org.apache.avro.Schema schema = ReflectData.get().getSchema(MyBean.class);
        String schemaDef = schema.toString(false);
        log.info("schema {}", schemaDef);

        List<MyBean> written = new ArrayList<>();
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < numMessages; i++) {
            MyBean bean = new MyBean();
            bean.setField("value" + i);
            String serialized = serializeBeanUsingAvro(schema, bean);
            payload.append(serialized);
            if (i != numMessages - 1) {
                // do not add a newline in the end of the file
                payload.append("\n");
            }
            written.add(bean);
        }

        // write messages to Kafka using kafka-avro-console-producer
        // we are writing the serialized values to the stdin of kafka-avro-console-producer
        // the only way to do it with TestContainers is actually to create a bash script
        // and execute it
        String bashFileTemplate = "echo '"+payload+"' " +
                "| /usr/bin/kafka-avro-console-producer " +
                "--broker-list " + getBootstrapServersOnDockerNetwork() + " " +
                "--property 'value.schema=" + schemaDef + "' " +
                "--property schema.registry.url="+ getRegistryAddressInDockerNetwork() +" " +
                "--topic "+ topic;
        String file = "/home/appuser/produceRecords.sh";

        schemaRegistryContainer.copyFileToContainer(Transferable
                        .of(bashFileTemplate.getBytes(StandardCharsets.UTF_8), 0777), file);

        ExecResult cat = schemaRegistryContainer.execInContainer("cat", file);
        log.info("cat results: {}", cat.getStdout());
        log.info("cat stderr: {}", cat.getStderr());

        ExecResult execResult = schemaRegistryContainer.execInContainer("/bin/bash", file);

        log.info("script results: {}", execResult.getStdout());
        log.info("script stderr: {}", execResult.getStderr());
        assertTrue(execResult.getStdout().contains("Closing the Kafka producer"), execResult.getStdout()+" "+execResult.getStderr());
        assertTrue(execResult.getStderr().isEmpty(), execResult.getStderr());

        log.info("Successfully produced {} messages to kafka topic {}", numMessages, topic);
        return written;
    }

    public void produceProtoMessages(int numMessages, String topic) throws Exception{
        String schemaDef = "syntax = \"proto3\";\n"
                + "package com.example;\n"
                + "\n"
                + "message MyMessage {\n"
                + "  string name = 1;\n"
                + "  int32 number = 2;\n"
                + "}";

        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < numMessages; i++) {
            String serialized = String.format("{\"name\":\"%s\",\"number\":%d}", "pulsar_" + i, i);
            payload.append(serialized);
            if (i != numMessages - 1) {
                // do not add a newline in the end of the file
                payload.append("\n");
            }
        }

        // write messages to Kafka using kafka-protobuf-console-producer
        // we are writing the serialized values to the stdin of kafka-avro-console-producer
        // the only way to do it with TestContainers is actually to create a bash script
        // and execute it
        String bashFileTemplate = "echo '"+payload+"' " +
                "| /usr/bin/kafka-protobuf-console-producer " +
                "--broker-list " + getBootstrapServersOnDockerNetwork() + " " +
                "--property 'value.schema=" + schemaDef + "' " +
                "--property schema.registry.url="+ getRegistryAddressInDockerNetwork() +" " +
                "--topic "+ topic;
        String file = "/home/appuser/produceRecords.sh";

        schemaRegistryContainer.copyFileToContainer(Transferable
                .of(bashFileTemplate.getBytes(StandardCharsets.UTF_8), 0777), file);

        ExecResult cat = schemaRegistryContainer.execInContainer("cat", file);
        log.info("cat results: {}", cat.getStdout());
        log.info("cat stderr: {}", cat.getStderr());

        ExecResult execResult = schemaRegistryContainer.execInContainer("/bin/bash", file);

        log.info("script results: {}", execResult.getStdout());
        log.info("script stderr: {}", execResult.getStderr());
        assertTrue(execResult.getStdout().contains("Closing the Kafka producer"), execResult.getStdout()+" "+execResult.getStderr());
        assertTrue(execResult.getStderr().isEmpty(), execResult.getStderr());

        log.info("Successfully produced {} messages to kafka topic {}", numMessages, topic);
    }

    private static String serializeBeanUsingAvro(org.apache.avro.Schema schema, MyBean bean) throws IOException {
        DatumWriter<MyBean> userDatumWriter = new ReflectDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, stream);
        userDatumWriter.write(bean, encoder);
        encoder.flush();
        String serialized = new String(stream.toByteArray(), StandardCharsets.UTF_8);
        return serialized;
    }

    protected void waitForProcessingSourceMessages(String tenant,
                                                   String namespace,
                                                   String sourceName,
                                                   int numMessages) throws Exception {
        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source",
                "status",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName
        };

        final ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        log.info("Get source status : {}", result.getStdout());

        assertEquals(result.getExitCode(), 0);

        SourceStatus sourceStatus = SourceStatusUtil.decode(result.getStdout());
        assertEquals(sourceStatus.getNumInstances(), 1);
        assertEquals(sourceStatus.getNumRunning(), 1);
        assertEquals(sourceStatus.getInstances().size(), 1);
        assertEquals(sourceStatus.getInstances().get(0).getInstanceId(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().isRunning(), true);
        assertTrue(sourceStatus.getInstances().get(0).getStatus().getLastReceivedTime() > 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumReceivedFromSource(), numMessages);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumWritten(), numMessages);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getNumRestarts(), 0);
        assertEquals(sourceStatus.getInstances().get(0).getStatus().getLatestSystemExceptions().size(), 0);
    }

    protected void deleteSource(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source",
                "delete",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName
        };

        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("Delete source successfully"),
                result.getStdout()
        );
        assertTrue(
                result.getStderr().isEmpty(),
                result.getStderr()
        );
    }

    protected void getSourceInfoNotFound(String tenant, String namespace, String sourceName) throws Exception {

        final String[] commands = {
                PulsarCluster.ADMIN_SCRIPT,
                "source",
                "get",
                "--tenant", tenant,
                "--namespace", namespace,
                "--name", sourceName
        };

        try {
            pulsarCluster.getAnyWorker().execCmd(commands);
            fail("Command should have exited with non-zero");
        } catch (ContainerExecException e) {
            assertTrue(e.getResult().getStderr().contains("Reason: Source " + sourceName + " doesn't exist"));
        }
    }

    public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
        private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;

        public SchemaRegistryContainer(String boostrapServers) throws Exception {
            super("confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION);

            addEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", boostrapServers);
            addEnv("SCHEMA_REGISTRY_HOST_NAME", schemaRegistryContainerName);

            withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
            withLogConsumer(o -> {
                log.info("schemaregistry> {}", o.getUtf8String());
            });
            waitingFor(Wait.forHttp("/subjects"));
        }
    }

    private String getRegistryAddressInDockerNetwork() {
        return "http://"+schemaRegistryContainerName + ":8081";
    }

}
