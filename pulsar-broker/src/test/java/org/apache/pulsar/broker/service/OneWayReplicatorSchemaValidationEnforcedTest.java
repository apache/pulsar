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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import lombok.Data;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class OneWayReplicatorSchemaValidationEnforcedTest extends OneWayReplicatorTestBase {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Data
    private static class MyClass {
        int field1;
        String field2;
        Long field3;
    }

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setSchemaValidationEnforced(true);
        // disable topic auto creation so that it's possible to reproduce the scenario consistently
        config.setAllowAutoTopicCreation(false);
    }

    @Test(timeOut = 30000)
    public void testReplicationWithAvroSchemaWithSchemaValidationEnforced() throws Exception {
        Schema<MyClass> myClassSchema = Schema.AVRO(MyClass.class);
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://" + sourceClusterAlwaysSchemaCompatibleNamespace + "/tp_");
        // create the topic and schema in the local cluster (r1)
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.schemas().createSchema(topicName, myClassSchema.getSchemaInfo());
        // create the topic and schema in the remote cluster (r2)
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.schemas().createSchema(topicName, myClassSchema.getSchemaInfo());

        // consume from the remote cluster (r2)
        org.apache.pulsar.client.api.Consumer<MyClass> consumer2 = client2.newConsumer(myClassSchema)
                .topic(topicName).subscriptionName("sub").subscribe();

        // produce to local cluster (r1)
        Producer<MyClass> producer1 = client1.newProducer(myClassSchema).topic(topicName).create();
        MyClass sentBody = new MyClass();
        sentBody.setField1(1);
        sentBody.setField2("test");
        sentBody.setField3(123456789L);
        producer1.send(sentBody);

        // verify that the message was received from the remote cluster (r2)
        Message<MyClass> received = consumer2.receive(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.getValue()).isNotNull().satisfies(receivedBody -> {
            assertThat(receivedBody.getField1()).isEqualTo(1);
            assertThat(receivedBody.getField2()).isEqualTo("test");
            assertThat(receivedBody.getField3()).isEqualTo(123456789L);
        });
    }

    @Test(timeOut = 30000)
    public void testReplicationReconnectWithSchemaValidationEnforced() throws Exception {
        Schema<MyClass> myClassSchema = Schema.AVRO(MyClass.class);
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://" + sourceClusterAlwaysSchemaCompatibleNamespace + "/tp_");
        // With the creation of the topic, replication will be initiated.
        // The schema that the internal producer of replication holes will be "Auto_producer -> bytes".
        admin1.topics().createNonPartitionedTopic(topicName);
        waitReplicatorStarted(topicName);
        // Registers new scheme on the remote-side.
        admin2.schemas().createSchema(topicName, myClassSchema.getSchemaInfo());
        // After a reconnection, the schema that the internal producer of replication holes should be
        // "Auto_producer -> myClassSchema".
        ServerCnx serverCnx2 = (ServerCnx) pulsar2.getBrokerService().getTopic(topicName, false).get().get()
                .getProducers().values().iterator().next().getCnx();
        serverCnx2.ctx().channel().close();
        // Verify: the producer reconnected successfully.
        Thread.sleep(2000);
        waitReplicatorStarted(topicName);
    }

}
