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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.BrokerServiceInternalMethodInvoker;
import org.apache.pulsar.broker.service.OneWayReplicatorTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.api.MessageId;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-replication")
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
    public void testReplicationTaskStoppedAfterTopicClosed() throws Exception {
        // Close a topic, which has enabled replication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        waitReplicatorStarted(topicName);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                .join().get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
        admin1.topics().unload(topicName);

        // Inject a task into the "inFlightTasks" to calculate how many times the method "replicator.readMoreEntries"
        // has been called.
        AtomicInteger counter = new AtomicInteger();
        InFlightTask injectedTask = new InFlightTask(PositionImpl.get(1, 1), 1, replicator.getReplicatorId());
        injectedTask.setEntries(Collections.emptyList());
        InFlightTask spyTask = spy(injectedTask);
        replicator.inFlightTasks.add(spyTask);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                counter.incrementAndGet();
                return invocationOnMock.callRealMethod();
            }
        }).when(spyTask).getReadPos();

        // Verify: there is no scheduled task to retry to read entries to replicate.
        // Call "readMoreEntries" to make the issue happen.
        replicator.readMoreEntries();
        Thread.sleep(PersistentTopic.MESSAGE_RATE_BACKOFF_MS * 10);
        assertEquals(replicator.getState(), AbstractReplicator.State.Terminated);
        assertTrue(counter.get() <= 1);
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
        PositionImpl position1 = new PositionImpl(1, 1);
        Assert.assertNotNull(position1, "Position should not be null");
        InFlightTask task1 = replicator.createOrRecycleInFlightTaskIntoQueue(position1, 10);
        // Verify a new task was created and added to the queue
        Assert.assertNotNull(task1, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should have one task");
        Assert.assertEquals(task1.getReadPos(), position1, "Task should have the correct position");
        Assert.assertEquals(task1.getReadingEntries(), 10, "Task should have the correct reading entries count");
        // Mark the task as done to test recycling
        task1.setEntries(Collections.emptyList());

        // Test Case 2: Recycle an existing task
        PositionImpl position2 = new PositionImpl(2, 2);
        Assert.assertNotNull(position2, "Position should not be null");
        InFlightTask task2 = replicator.createOrRecycleInFlightTaskIntoQueue(position2, 20);
        // Verify the task was recycled
        Assert.assertNotNull(task2, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should still have one task");
        Assert.assertEquals(task2.getReadPos(), position2, "Task should have the updated position");
        Assert.assertEquals(task2.getReadingEntries(), 20, "Task should have the updated reading entries count");

        // Test Case 3: Create a new task when no tasks can be recycled
        task2.setEntries(null); // Make the task not done
        PositionImpl position3 = new PositionImpl(3, 3);
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
        log.info("Starting testGetInflightMessagesCount");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            // Test Case 1: no task.
            Assert.assertEquals(replicator.getInflightMessagesCount(), 0);

            // Test Case 2: cursor reading.
            PositionImpl position1 = new PositionImpl(1, 1);
            InFlightTask task1 = new InFlightTask(position1, 3, "");
            inFlightTasks.add(task1);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 3);

            // Test Case 3: read completed.
            inFlightTasks.clear();
            PositionImpl position2 = new PositionImpl(2, 2);
            InFlightTask task2 = new InFlightTask(position2, 3, "");
            task2.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class)));
            inFlightTasks.add(task2);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            // Test Case 4: Task with some completed entries
            task2.setCompletedEntries(1);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 1);

            // Test Case 5: Task with all entries completed
            task2.setCompletedEntries(2);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 0);

            // Test Case 6: Multiple tasks with different states
            // task2 has 0 in-flight (2 completed out of 2)
            // task3 has 2 in-flight (1 completed out of 3)
            PositionImpl position3 = new PositionImpl(3, 3);
            InFlightTask task3 = new InFlightTask(position3, 4, "");
            task3.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            task3.setCompletedEntries(1);
            inFlightTasks.add(task3);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            // Test Case 7: Multiple tasks with different states
            // task2 has 0 in-flight (2 completed out of 2)
            // task3 has 2 in-flight (1 completed out of 3)
            // task4 has 0 in-flight (empty readoutEntries)
            PositionImpl position4 = new PositionImpl(4, 4);
            InFlightTask task4 = new InFlightTask(position4, 2, "");
            task4.setEntries(Collections.emptyList());
            inFlightTasks.add(task4);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            log.info("Completed testGetInflightMessagesCount");
        } finally {
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    @Test
    public void testGetPermitsIfNoPendingRead() throws Exception {
        log.info("Starting testGetPermitsIfNoPendingRead");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            // Test Case 1: Empty queue - should return producerQueueSize (1000)
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000,
                    "With empty queue, should return full producerQueueSize");

            // Test Case 2: Task with pending read (readPos != null && readoutEntries == null)
            PositionImpl position1 = new PositionImpl(1, 1);
            InFlightTask pendingReadTask = new InFlightTask(position1, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "With pending read task, should return 0");

            // Test Case 3: Task with completed read but in-flight messages
            inFlightTasks.clear();
            PositionImpl position2 = new PositionImpl(2, 2);
            InFlightTask completedReadTask = new InFlightTask(position2, 5, "");
            completedReadTask.setEntries(Arrays.asList(
                    mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            inFlightTasks.add(completedReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 3,
                    "With completed read task, should return producerQueueSize - inflightMessages");

            // Test Case 4: Multiple tasks with no pending reads
            PositionImpl position3 = new PositionImpl(3, 3);
            InFlightTask task2 = new InFlightTask(position3, 5, "");
            task2.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class)));
            task2.setCompletedEntries(1); // 1 in-flight message
            inFlightTasks.add(task2);
            // Now we have 3 + 1 = 4 in-flight messages
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 4,
                    "With multiple tasks, should return producerQueueSize - total inflightMessages");

            // Test Case 5: Multiple tasks including one with pending read
            PositionImpl position4 = new PositionImpl(4, 4);
            InFlightTask pendingReadTask2 = new InFlightTask(position4, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask2);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "With any pending read task, should return 0 regardless of other tasks");

            log.info("Completed testGetPermitsIfNoPendingRead");
        } finally {
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    @Test
    public void testAcquirePermitsIfNotFetchingSchema() throws Exception {
        log.info("Starting testAcquirePermitsIfNotFetchingSchema");
        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        // Save original state
        int originalWaitForCursorRewinding = replicator.waitForCursorRewindingRefCnf;
        AbstractReplicator.State originalState = replicator.getState();

        try {
            // Test Case 1: Normal case - no pending read, not waiting for cursor rewinding, state is Started
            // Should return a new InFlightTask
            // First, check the current permits available
            int expectedPermits = replicator.getPermitsIfNoPendingRead();
            Assert.assertTrue(expectedPermits > 0, "Should have available permits for the test");
            InFlightTask task1 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNotNull(task1, "Should return a new InFlightTask in normal case");
            Assert.assertNotNull(task1.getReadPos(), "Task should have a read position");
            Assert.assertEquals(task1.getReadingEntries(), expectedPermits,
                    "Task readingEntries should equal the number of permits available");
            Assert.assertTrue(inFlightTasks.contains(task1),
                    "Task should be added to the inFlightTasks list");

            // Test Case 2: With pending read - should return null
            inFlightTasks.clear();
            PositionImpl position1 = new PositionImpl(1, 1);
            InFlightTask pendingReadTask = new InFlightTask(position1, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask);
            InFlightTask task2 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task2, "Should return null when there is a pending read");

            // Test Case 3: With waitForCursorRewinding=true - should return null
            inFlightTasks.clear();
            replicator.waitForCursorRewindingRefCnf = 1;
            InFlightTask task3 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task3, "Should return null when waiting for cursor rewinding");
            // Reset for next test
            replicator.waitForCursorRewindingRefCnf = 0;

            // Test Case 4: With state != Started - should return null
            // We need to use reflection to modify the state since it's protected by AtomicReferenceFieldUpdater
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, AbstractReplicator.State.Starting);
            InFlightTask task4 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task4, "Should return null when state is not Started");
            // Reset state for next test
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, AbstractReplicator.State.Started);

            // Test Case 5: With limited permits - verify readingEntries is set correctly
            inFlightTasks.clear();
            // Add a task with some in-flight messages to reduce available permits
            PositionImpl positionLimited = new PositionImpl(10, 10);
            InFlightTask limitedTask = new InFlightTask(positionLimited, 5, "");
            // Add enough entries to leave just a small number of permits (e.g., 10)
            List<Entry> limitedEntries = new ArrayList<>();
            int entriesCount = 990;
            for (int j = 0; j < entriesCount; j++) {
                limitedEntries.add(mock(Entry.class));
            }
            limitedTask.setEntries(limitedEntries);
            inFlightTasks.add(limitedTask);
            // Check that we have limited permits available
            int limitedPermits = replicator.getPermitsIfNoPendingRead();
            Assert.assertTrue(limitedPermits > 0 && limitedPermits < 20,
                    "Should have a small number of permits available for testing");
            // Now acquire permits and verify readingEntries matches the limited permits
            InFlightTask task5 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNotNull(task5, "Should return a task with limited permits");
            Assert.assertEquals(task5.getReadingEntries(), limitedPermits,
                    "Task readingEntries should equal the limited number of permits available");

            // Test Case 6: With permits=0 - should return null
            inFlightTasks.clear();
            // Add tasks that will make getPermitsIfNoPendingRead() return 0
            // We need enough in-flight messages to equal producerQueueSize
            for (int i = 0; i < 10; i++) {
                PositionImpl position = new PositionImpl(i, i);
                InFlightTask task = new InFlightTask(position, 5, "");
                List<Entry> entries = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    entries.add(mock(Entry.class));
                }
                task.setEntries(entries);
                inFlightTasks.add(task);
            }
            InFlightTask task6 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task6, "Should return null when permits is 0");
            log.info("Completed testAcquirePermitsIfNotFetchingSchema");
        } finally {
            // Restore original state
            replicator.waitForCursorRewindingRefCnf = originalWaitForCursorRewinding;
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, originalState);
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }
}
