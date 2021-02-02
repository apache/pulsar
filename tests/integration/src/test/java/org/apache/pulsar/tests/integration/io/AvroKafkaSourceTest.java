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
package org.apache.pulsar.tests.integration.io;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.google.gson.Gson;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.io.kafka.KafkaAvroRecordSource;
import org.apache.pulsar.tests.integration.docker.ContainerExecException;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.functions.PulsarFunctionsTestBase;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.common.policies.data.TenantInfo;

import static org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase.randomName;
import static org.testng.Assert.*;

/**
 * A tester for testing kafka source with Avro Messages.
 */
@Slf4j
public class AvroKafkaSourceTest extends PulsarFunctionsTestBase {

    private static final String SOURCE_TYPE = "kafka";

    final Duration ONE_MINUTE = Duration.ofMinutes(1);
    final Duration TEN_SECONDS = Duration.ofSeconds(10);

    final RetryPolicy statusRetryPolicy = new RetryPolicy()
            .withMaxDuration(ONE_MINUTE)
            .withDelay(TEN_SECONDS)
            .onRetry(e -> log.error("Retry ... "));

    private final String kafkaTopicName;

    private KafkaContainer kafkaContainer;

    private KafkaConsumer<String, String> kafkaConsumer;

    protected final String sourceType;
    protected final Map<String, Object> sourceConfig;
    protected final String networkAlias;
    protected final String containerName;

    public AvroKafkaSourceTest() {
        this.containerName = "kafka-" + randomName(8);
        this.networkAlias = containerName;
        sourceType = SOURCE_TYPE;
        sourceConfig = new HashMap<>();
        this.kafkaTopicName = "kafkasourcetopic";

        sourceConfig.put("bootstrapServers", containerName + ":9092");
        sourceConfig.put("groupId", "test-source-group");
        sourceConfig.put("fetchMinBytes", 1L);
        sourceConfig.put("autoCommitIntervalMs", 10L);
        sourceConfig.put("sessionTimeoutMs", 10000L);
        sourceConfig.put("heartbeatIntervalMs", 5000L);
        sourceConfig.put("topic", kafkaTopicName);
    }

    @Test(groups = "source")
    public void test() throws Exception {
        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }
        KafkaContainer serviceContainer = startServiceContainer(pulsarCluster);
        try {
            testSource();
        } finally {
            stopServiceContainer(pulsarCluster);
        }
    }

    public KafkaContainer startServiceContainer(PulsarCluster cluster) {
        this.kafkaContainer = createSinkService(cluster);
        cluster.startService(networkAlias, kafkaContainer);
        return kafkaContainer;
    }

    protected KafkaContainer createSinkService(PulsarCluster cluster) {
        return new KafkaContainer()
                .withEmbeddedZookeeper()
                .withNetworkAliases(containerName)
                .withCreateContainerCmdModifier(createContainerCmd -> createContainerCmd
                        .withName(containerName)
                        .withHostName(cluster.getClusterName() + "-" + containerName));
    }

    public void stopServiceContainer(PulsarCluster cluster) {
        if (null != kafkaContainer) {
            cluster.stopService(networkAlias, kafkaContainer);
        }
    }

    public void prepareSource() throws Exception {
        log.info("creating topic");
        ExecResult execResult = kafkaContainer.execInContainer(
            "/usr/bin/kafka-topics",
            "--create",
            "--zookeeper",
            "localhost:2181",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--topic",
            kafkaTopicName);
        assertTrue(
            execResult.getStdout().contains("Created topic"),
            execResult.getStdout());

        kafkaConsumer = new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "source-test-" + randomName(8),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );
        kafkaConsumer.subscribe(Arrays.asList(kafkaTopicName));
        log.info("Successfully subscribe to kafka topic {}", kafkaTopicName);



    }

    private <T extends GenericContainer> void testSource()  throws Exception {

        if (pulsarCluster == null) {
            super.setupCluster();
            super.setupFunctionWorkers();
        }

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "test-source-connector-"
                + functionRuntimeType + "-output-topic-" + randomName(8);
        final String sourceName = "test-source-connector-"
                + functionRuntimeType + "-name-" + randomName(8);
        final int numMessages = 20;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        admin.topics().createNonPartitionedTopic(outputTopicName);

       /* @Cleanup
        Consumer<GenericRecord> consumer = client.newConsumer(Schema.AUTO_CONSUME())
                .topic(outputTopicName)
                .subscriptionName("source-tester")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();*/

        // prepare the testing environment for source
        prepareSource();

        // submit the source connector
        submitSourceConnector(tenant, namespace, sourceName, outputTopicName);

        // get source info
        getSourceInfoSuccess(tenant, namespace, sourceName);

        // get source status
        Failsafe.with(statusRetryPolicy).run(() -> getSourceStatus(tenant, namespace, sourceName));

        // produce messages
        Map<String, String> kvs = produceSourceMessages(numMessages);

        // wait for source to process messages
        Failsafe.with(statusRetryPolicy).run(() ->
                waitForProcessingSourceMessages(tenant, namespace, sourceName, numMessages));

        // validate the source result
//       validateSourceResultAvro(consumer, kvs.size());

        // delete the source
        deleteSource(tenant, namespace, sourceName);

        // get source info (source should be deleted)
        getSourceInfoNotFound(tenant, namespace, sourceName);
    }

    public void validateSourceResultAvro(Consumer<GenericRecord> consumer,
                                         int number) throws Exception {
        int recordsNumber = 0;
        Message<GenericRecord> msg = consumer.receive(2, TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            GenericRecord valueRecord = msg.getValue();
            Assert.assertNotNull(valueRecord.getFields());
            Assert.assertTrue(valueRecord.getFields().size() > 0);
            for (Field field : valueRecord.getFields()) {
                log.info("field {} value {}", field, valueRecord.getField(field));
            }

            consumer.acknowledge(msg);
            msg = consumer.receive(1, TimeUnit.SECONDS);
        }

        Assert.assertEquals(recordsNumber, number);
        log.info("Stop {} server container. topic: {} has {} records.", sourceType, consumer.getTopic(), recordsNumber);
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
                result.getStdout().contains("\"archive\": \"builtin://" + sourceType + "\""),
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

        final SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());

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
                "--classname", "org.apache.pulsar.io.kafka.KafkaAvroRecordSource",
                "--source-type", sourceType,
                "--sourceConfig", new Gson().toJson(sourceConfig),
                "--destinationTopicName", outputTopicName
        };

        log.info("Run command : {}", StringUtils.join(commands, ' '));
        ContainerExecResult result = pulsarCluster.getAnyWorker().execCmd(commands);
        assertTrue(
                result.getStdout().contains("\"Created successfully\""),
                result.getStdout());
    }

    @Data
    public static final class MyBean {
        private String field;
    }

    public Map<String, MyBean> produceSourceMessages(int numMessages) throws Exception{

        pp.put("schema.registry.url", "http://localhost:8081");
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());

        final AvroMapper avroMapper = new AvroMapper();
        final AvroSchema schema = avroMapper.schemaFor(MyBean.class);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString(),
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  KafkaAvroSerializer.class.getName()
                )
        );
        LinkedHashMap<String, MyBean> kvs = new LinkedHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            MyBean value = new MyBean();
            value.setField("value-"+i);
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema.getAvroSchema());
            recordBuilder.set("field", value.field);
            final GenericData.Record genericRecord = recordBuilder.build();
            final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(kafkaTopicName,
                    "customer", genericRecord);
            producer.send(producerRecord).get();
            kvs.put(key, value);
        }

        log.info("Successfully produced {} messages to kafka topic {}", numMessages, kafkaTopicName);
        return kvs;
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

        SourceStatus sourceStatus = SourceStatus.decode(result.getStdout());
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
}
