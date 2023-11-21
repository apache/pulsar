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
package org.apache.pulsar.broker.service.schema;

import static org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicSchemaTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "topicDomains")
    public Object[][] topicDomains() {
        return new Object[][]{
                {TopicDomain.non_persistent},
                {TopicDomain.persistent}
        };
    }

    @Test(dataProvider = "topicDomains")
    public void testDeleteNonPartitionedTopicWithSchema(TopicDomain topicDomain) throws Exception {
        final String topic = BrokerTestUtil.newUniqueName(topicDomain.value() + "://public/default/tp");
        final String schemaId = TopicName.get(TopicName.get(topic).getPartitionedTopicName()).getSchemaName();
        admin.topics().createNonPartitionedTopic(topic);

        // Add schema.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                .enableBatching(false).create();
        producer.close();
        List<SchemaAndMetadata> schemaList1 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList1 != null && schemaList1.size() > 0);

        // Verify the schema has been deleted with topic.
        admin.topics().delete(topic, false, true);
        List<SchemaAndMetadata> schemaList2 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList2 == null || schemaList2.isEmpty());
    }

    @Test
    public void testDeletePartitionedTopicWithoutSchema() throws Exception {
        // Non-persistent topic does not support partitioned topic now, so only write a test case for persistent topic.
        TopicDomain topicDomain = TopicDomain.persistent;
        final String topic = BrokerTestUtil.newUniqueName(topicDomain.value() + "://public/default/tp");
        final String partition0 = topic + "-partition-0";
        final String partition1 = topic + "-partition-1";
        final String schemaId = TopicName.get(TopicName.get(topic).getPartitionedTopicName()).getSchemaName();
        admin.topics().createPartitionedTopic(topic, 2);

        // Add schema.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                .enableBatching(false).create();
        producer.close();
        List<SchemaAndMetadata> schemaList1 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList1 != null && schemaList1.size() > 0);

        // Verify the schema will not been deleted with partition-0.
        admin.topics().delete(partition0, false, true);
        List<SchemaAndMetadata> schemaList2 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList2 != null && schemaList2.size() > 0);

        // Verify the schema will not been deleted with partition-0 & partition-1.
        admin.topics().delete(partition1, false, true);
        List<SchemaAndMetadata> schemaList3 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList3 != null && schemaList3.size() > 0);

        // Verify the schema will be deleted with partitioned metadata.
        admin.topics().deletePartitionedTopic(topic, false, true);
        List<SchemaAndMetadata> schemaList4 = pulsar.getSchemaRegistryService().getAllSchemas(schemaId).join()
                .stream().map(s -> s.join()).filter(Objects::nonNull).collect(Collectors.toList());
        assertTrue(schemaList4 == null || schemaList4.isEmpty());
    }
}
