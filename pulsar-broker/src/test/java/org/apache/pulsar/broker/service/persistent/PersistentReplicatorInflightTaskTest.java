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
package org.apache.pulsar.broker.service.persistent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.OneWayReplicatorTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.api.MessageId;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PersistentReplicatorInflightTaskTest extends OneWayReplicatorTestBase {

    private final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
    private final String subscriptionName = "s1";

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        createTopics();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private void createTopics() throws Exception {
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
    }

    @Test
    public void testCreateOrRecycleInFlightTaskIntoQueue() throws Exception {
        log.info("Starting testCreateOrRecycleInFlightTaskIntoQueue");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for verification
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Clear any existing tasks to start with a clean state
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        // Test Case 1: Create a new task when the queue is empty
        Position position1 = PositionFactory.create(1, 1);
        Assert.assertNotNull(position1, "Position should not be null");
        InFlightTask task1 = replicator.createOrRecycleInFlightTaskIntoQueue(position1, 10);
        // Verify a new task was created and added to the queue
        Assert.assertNotNull(task1, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should have one task");
        Assert.assertEquals(task1.getReadPos(), position1, "Task should have the correct position");
        Assert.assertEquals(task1.getReadingEntries(), 10, "Task should have the correct reading entries count");
        // Mark the task as done to test recycling
        task1.setReadoutEntries(Collections.emptyList());

        // Test Case 2: Recycle an existing task
        Position position2 = PositionFactory.create(2, 2);
        Assert.assertNotNull(position2, "Position should not be null");
        InFlightTask task2 = replicator.createOrRecycleInFlightTaskIntoQueue(position2, 20);
        // Verify the task was recycled
        Assert.assertNotNull(task2, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should still have one task");
        Assert.assertEquals(task2.getReadPos(), position2, "Task should have the updated position");
        Assert.assertEquals(task2.getReadingEntries(), 20, "Task should have the updated reading entries count");

        // Test Case 3: Create a new task when no tasks can be recycled
        task2.setReadoutEntries(null); // Make the task not done
        Position position3 = PositionFactory.create(3, 3);
        Assert.assertNotNull(position3, "Position should not be null");
        InFlightTask task3 = replicator.createOrRecycleInFlightTaskIntoQueue(position3, 30);
        // Verify a new task was created
        Assert.assertNotNull(task3, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 2, "Queue should have two tasks");
        Assert.assertEquals(task3.getReadPos(), position3, "Task should have the correct position");
        Assert.assertEquals(task3.getReadingEntries(), 30, "Task should have the correct reading entries count");

        // cleanup.
        log.info("Completed testCreateOrRecycleInFlightTaskIntoQueue");
        inFlightTasks.clear();
        inFlightTasks.addAll(originalTasks);
    }

    @Test
    public void testGetInflightMessagesCount() throws Exception {

    }
}
