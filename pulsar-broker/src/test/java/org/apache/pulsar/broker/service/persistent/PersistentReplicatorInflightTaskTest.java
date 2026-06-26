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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerTest;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.OneWayReplicatorTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.ProducerSendCallback;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.ReasonOfWaitForCursorRewinding;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
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
        waitReplicatorStarted(topicName, pulsar2);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                .join().get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
        admin1.topics().unload(topicName);

        // Inject a task into the "inFlightTasks" to calculate how many times the method "replicator.readMoreEntries"
        // has been called.
        AtomicInteger counter = new AtomicInteger();
        InFlightTask injectedTask = new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
        injectedTask.setEntries(Collections.emptyList());
        InFlightTask spyTask = spy(injectedTask);
        replicator.inFlightTasks.add(spyTask);
        doAnswer(new Answer<Object>() {
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
    public void testReadEntriesFailedCompletesInFlightTaskAfterReplicatorTerminated() throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch failRead = new CountDownLatch(1);
        Producer<String> producer = null;
        try {
            admin1.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createNonPartitionedTopic(topicName);
            producer = client1.newProducer(Schema.STRING).topic(topicName).create();
            producer.send("msg");

            PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                    .join().get();
            ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
            ManagedLedgerTest.makeReadEntryProbFail(ml, () -> {
                readStarted.countDown();
                try {
                    if (!failRead.await(30, TimeUnit.SECONDS)) {
                        return new ManagedLedgerException("Timed out waiting to fail read entries");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new ManagedLedgerException(e);
                }
                return new ManagedLedgerException.TooManyRequestsException("mocked read failure");
            });

            pulsar1.getConfig().setReplicationStartAt("earliest");
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            assertTrue(readStarted.await(30, TimeUnit.SECONDS));

            PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
            Assert.assertNotNull(replicator, "Replicator should not be null");
            Assert.assertTrue(replicator.hasPendingRead());

            replicator.terminate();
            Assert.assertTrue(replicator.getState() != AbstractReplicator.State.Started);
            failRead.countDown();

            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                    Assert.assertFalse(replicator.hasPendingRead()));
        } finally {
            failRead.countDown();
            if (producer != null) {
                producer.close();
            }
            admin1.topics().delete(topicName, true);
            admin2.topics().delete(topicName, true);
        }
    }

    @Test
    public void testFailedPublishCompletesInFlightTask() throws Exception {
        PersistentReplicator replicator = spy(getReplicator(topicName));
        doNothing().when(replicator).beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Failed_Publishing);
        doNothing().when(replicator).doRewindCursor(false);
        doNothing().when(replicator).readMoreEntries();

        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            InFlightTask task = new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
            task.setEntries(Collections.singletonList(mock(Entry.class)));
            inFlightTasks.add(task);
            assertEquals(replicator.getPermitsIfNoPendingRead(), 999);

            ProducerSendCallback callback = ProducerSendCallback.create(replicator, mock(Entry.class), null, task);
            callback.sendComplete(new PulsarClientException.ProducerBlockedQuotaExceededException("mocked"), null);

            assertTrue(task.isDone());
            assertEquals(replicator.getPermitsIfNoPendingRead(), 1000);
        } finally {
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
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
        task1.setEntries(Collections.emptyList());

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
        task2.setEntries(null); // Make the task not done
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
            Position position1 = PositionFactory.create(1, 1);
            InFlightTask task1 = new InFlightTask(position1, 3, "");
            inFlightTasks.add(task1);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 3);

            // Test Case 3: read completed.
            inFlightTasks.clear();
            Position position2 = PositionFactory.create(2, 2);
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
            Position position3 = PositionFactory.create(3, 3);
            InFlightTask task3 = new InFlightTask(position3, 4, "");
            task3.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            task3.setCompletedEntries(1);
            inFlightTasks.add(task3);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            // Test Case 7: Multiple tasks with different states
            // task2 has 0 in-flight (2 completed out of 2)
            // task3 has 2 in-flight (1 completed out of 3)
            // task4 has 0 in-flight (empty readoutEntries)
            Position position4 = PositionFactory.create(4, 4);
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
            Position position1 = PositionFactory.create(1, 1);
            InFlightTask pendingReadTask = new InFlightTask(position1, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "With pending read task, should return 0");

            // Test Case 3: Task with completed read but in-flight messages
            inFlightTasks.clear();
            Position position2 = PositionFactory.create(2, 2);
            InFlightTask completedReadTask = new InFlightTask(position2, 5, "");
            completedReadTask.setEntries(Arrays.asList(
                    mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            inFlightTasks.add(completedReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 3,
                    "With completed read task, should return producerQueueSize - inflightMessages");

            // Test Case 4: Multiple tasks with no pending reads
            Position position3 = PositionFactory.create(3, 3);
            InFlightTask task2 = new InFlightTask(position3, 5, "");
            task2.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class)));
            task2.setCompletedEntries(1); // 1 in-flight message
            inFlightTasks.add(task2);
            // Now we have 3 + 1 = 4 in-flight messages
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 4,
                    "With multiple tasks, should return producerQueueSize - total inflightMessages");

            // Test Case 5: Multiple tasks including one with pending read
            Position position4 = PositionFactory.create(4, 4);
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

    public static Runnable pauseReplicator(PersistentReplicator replicator) {
        Awaitility.await().untilAsserted(() -> {
            assertTrue(replicator.isConnected());
        });
        replicator.beforeTerminateOrCursorRewinding(PersistentReplicator.ReasonOfWaitForCursorRewinding.Disconnecting);
        replicator.doRewindCursor(false);
        InFlightTask inFlightTask =
                replicator.createOrRecycleInFlightTaskIntoQueue(PositionFactory.create(1, 1), 1);
        return () -> {
            inFlightTask.setEntries(Collections.emptyList());
            replicator.readMoreEntries();
        };
    }
}
