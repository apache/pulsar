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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CreateSubscriptionTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void createSubscriptionSingleTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-topic";
        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (ConflictException e) {
            assertEquals(((ClientErrorException) e.getCause()).getResponse().getStatus(),
                    Status.CONFLICT.getStatusCode());
        }

        assertEquals(admin.topics().getSubscriptions(topic), Lists.newArrayList("sub-1"));

        Producer<byte[]> p1 = pulsarClient.newProducer().topic(topic).create();
        p1.send("test-1".getBytes());
        p1.send("test-2".getBytes());
        MessageId m3 = p1.send("test-3".getBytes());

        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-1").getMsgBacklog(), 3);

        admin.topics().createSubscription(topic, "sub-2", MessageId.latest);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-2").getMsgBacklog(), 0);

        admin.topics().createSubscription(topic, "sub-3", MessageId.earliest);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-3").getMsgBacklog(), 3);

        admin.topics().createSubscription(topic, "sub-5", m3);
        assertEquals(admin.topics().getStats(topic).getSubscriptions().get("sub-5").getMsgBacklog(), 1);
    }

    @Test
    public void createSubscriptionOnPartitionedTopic() throws Exception {
        String topic = "persistent://my-property/my-ns/my-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(admin.topics().getSubscriptions(TopicName.get(topic).getPartition(i).toString()),
                    Lists.newArrayList("sub-1"));
        }
    }

    @Test
    public void createSubscriptionOnPartitionedTopicWithPartialFailure() throws Exception {
        String topic = "persistent://my-property/my-ns/my-partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        // create subscription for one partition
        final String partitionedTopic0 = topic+"-partition-0";
        admin.topics().createSubscription(partitionedTopic0, "sub-1", MessageId.latest);

        admin.topics().createSubscription(topic, "sub-1", MessageId.latest);

        // Create should fail if the subscription already exists
        try {
            admin.topics().createSubscription(topic, "sub-1", MessageId.latest);
            fail("Should have failed");
        } catch (Exception e) {
            // Expected
        }

        for (int i = 0; i < 10; i++) {
            assertEquals(
                    admin.topics().getSubscriptions(TopicName.get(topic).getPartition(i).toString()),
                    Lists.newArrayList("sub-1"));
        }
    }
}
