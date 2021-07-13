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
package org.apache.pulsar.tests.integration.io.sources.debezium;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.tests.integration.containers.DebeziumMongoDbContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumMySQLContainer;
import org.apache.pulsar.tests.integration.containers.DebeziumPostgreSqlContainer;
import org.apache.pulsar.tests.integration.io.PulsarIOTestBase;
import org.testcontainers.shaded.com.google.common.collect.Sets;
import org.testng.annotations.Test;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarDebeziumSourcesTest extends PulsarIOTestBase {

	protected final AtomicInteger testId = new AtomicInteger(0);

    @Test(groups = "source")
    public void testDebeziumMySqlSourceJson() throws Exception {
        testDebeziumMySqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    @Test(groups = "source")
    public void testDebeziumMySqlSourceAvro() throws Exception {
        testDebeziumMySqlConnect(
                "org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroConverter", false);
    }

    @Test(groups = "source")
    public void testDebeziumPostgreSqlSource() throws Exception {
        testDebeziumPostgreSqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    @Test(groups = "source")
    public void testDebeziumMongoDbSource() throws Exception{
        testDebeziumMongoDbConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    private void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
        boolean isJsonConverter = converterClassName.endsWith("JsonConverter");
        final String consumeTopicName = "debezium/mysql-"
                + (isJsonConverter ? "json" : "avro")
                + "/dbserver1.inventory.products";
        final String sourceName = "test-source-debezium-mysql" + (isJsonConverter ? "json" : "avro")
                + "-" + functionRuntimeType + "-" + randomName(8);

        // This is the binlog count that contained in mysql container.
        final int numMessages = 47;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
            .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        try {
            SchemaInfo lastSchemaInfo = admin.schemas().getSchemaInfo(consumeTopicName);
            log.info("lastSchemaInfo: {}", lastSchemaInfo == null ? "null" : lastSchemaInfo.toString());
        } catch (Exception e) {
            log.warn("failed to get schemaInfo for topic: {}, exceptions message: {}",
                    consumeTopicName, e.getMessage());
        }

        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        DebeziumMySqlSourceTester sourceTester = new DebeziumMySqlSourceTester(pulsarCluster, converterClassName);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mysql server
        DebeziumMySQLContainer mySQLContainer = new DebeziumMySQLContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(mySQLContainer);

        PulsarIODebeziumSourceRunner runner = new PulsarIODebeziumSourceRunner(pulsarCluster, functionRuntimeType.toString(),
        		converterClassName, tenant, namespace, sourceName, outputTopicName, numMessages, jsonWithEnvelope,
        		consumeTopicName, client);

        runner.testSource(sourceTester);
    }

	private void testDebeziumPostgreSqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
        final String consumeTopicName = "debezium/postgresql/dbserver1.inventory.products";
        final String sourceName = "test-source-debezium-postgersql-" + functionRuntimeType + "-" + randomName(8);


        // This is the binlog count that contained in postgresql container.
        final int numMessages = 26;

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        admin.topics().createNonPartitionedTopic(consumeTopicName);
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        DebeziumPostgreSqlSourceTester sourceTester = new DebeziumPostgreSqlSourceTester(pulsarCluster);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium postgresql server
        DebeziumPostgreSqlContainer postgreSqlContainer = new DebeziumPostgreSqlContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(postgreSqlContainer);

        PulsarIODebeziumSourceRunner runner = new PulsarIODebeziumSourceRunner(pulsarCluster, functionRuntimeType.toString(),
        		converterClassName, tenant, namespace, sourceName, outputTopicName, numMessages, jsonWithEnvelope,
        		consumeTopicName, client);

        runner.testSource(sourceTester);
    }

	private void testDebeziumMongoDbConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {

        final String tenant = TopicName.PUBLIC_TENANT;
        final String namespace = TopicName.DEFAULT_NAMESPACE;
        final String outputTopicName = "debe-output-topic-name";
        final String consumeTopicName = "debezium/mongodb/dbserver1.inventory.products";
        final String sourceName = "test-source-connector-"
                + functionRuntimeType + "-name-" + randomName(8);

        // This is the binlog count that contained in mongodb container.
        final int numMessages = 17;
        
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
        initNamespace(admin);

        admin.topics().createNonPartitionedTopic(consumeTopicName);
        admin.topics().createNonPartitionedTopic(outputTopicName);

        @Cleanup
        DebeziumMongoDbSourceTester sourceTester = new DebeziumMongoDbSourceTester(pulsarCluster);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mongodb server
        DebeziumMongoDbContainer mongoDbContainer = new DebeziumMongoDbContainer(pulsarCluster.getClusterName());
        sourceTester.setServiceContainer(mongoDbContainer);

        PulsarIODebeziumSourceRunner runner = new PulsarIODebeziumSourceRunner(pulsarCluster, functionRuntimeType.toString(),
        		converterClassName, tenant, namespace, sourceName, outputTopicName, numMessages, jsonWithEnvelope,
        		consumeTopicName, client);

        runner.testSource(sourceTester);
    }

    protected void initNamespace(PulsarAdmin admin) {
        log.info("[initNamespace] start.");
        try {
            admin.tenants().createTenant("debezium", new TenantInfoImpl(Sets.newHashSet(),
                    Sets.newHashSet(pulsarCluster.getClusterName())));
            admin.namespaces().createNamespace("debezium/mysql-json");
            admin.namespaces().createNamespace("debezium/mysql-avro");
            admin.namespaces().createNamespace("debezium/mongodb");
            admin.namespaces().createNamespace("debezium/postgresql");
        } catch (Exception e) {
            log.info("[initNamespace] msg: {}", e.getMessage());
        }
        log.info("[initNamespace] finish.");
    }
}
