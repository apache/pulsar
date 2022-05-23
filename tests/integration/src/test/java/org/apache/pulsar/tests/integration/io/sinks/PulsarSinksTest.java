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
package org.apache.pulsar.tests.integration.io.sinks;


import org.apache.pulsar.tests.integration.io.PulsarIOTestBase;
import org.apache.pulsar.tests.integration.io.RabbitMQSinkTester;
import org.apache.pulsar.tests.integration.io.RabbitMQSourceTester;
import org.apache.pulsar.tests.integration.io.sources.KafkaSourceTester;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PulsarSinksTest extends PulsarIOTestBase {

    @DataProvider(name = "withSchema")
    public Object[][] withSchema() {
        return new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}};
    }

    @Test(groups = "sink")
    public void testKafkaSink() throws Exception {
        final String kafkaContainerName = "kafka-" + randomName(8);
        testSink(new KafkaSinkTester(kafkaContainerName), true, new KafkaSourceTester(kafkaContainerName));
    }

    @Test(enabled = false, groups = "sink")
    public void testCassandraSink() throws Exception {
        testSink(CassandraSinkTester.createTester(true), true);
    }

    @Test(enabled = false, groups = "sink")
    public void testCassandraArchiveSink() throws Exception {
        testSink(CassandraSinkTester.createTester(false), false);
    }

    @Test(enabled = false, groups = "sink")
    public void testHdfsSink() throws Exception {
        testSink(new HdfsSinkTester(), false);
    }

    @Test(groups = "sink")
    public void testJdbcSink() throws Exception {
        testSink(new JdbcPostgresSinkTester(), true);
    }

    @Test(groups = "sink", dataProvider = "withSchema")
    public void testElasticSearch7Sink(boolean withSchema) throws Exception {
        testSink(new ElasticSearch7SinkTester(withSchema), true);
    }

    @Test(groups = "sink", dataProvider = "withSchema")
    public void testElasticSearch8Sink(boolean withSchema) throws Exception {
        testSink(new ElasticSearch8SinkTester(withSchema), true);
    }

    @Test(groups = "sink", dataProvider = "withSchema")
    public void testOpenSearchSinkRawData(boolean withSchema) throws Exception {
        testSink(new OpenSearchSinkTester(withSchema), true);
    }

    @Test(groups = "sink")
    public void testRabbitMQSink() throws Exception {
        final String containerName = "rabbitmq-" + randomName(8);
        testSink(new RabbitMQSinkTester(containerName), true, new RabbitMQSourceTester(containerName));
    }

    @Test(groups = "sink", dataProvider = "withSchema")
    public void testKinesis(boolean withSchema) throws Exception {
        testSink(new KinesisSinkTester(withSchema), true);
    }

}
