package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumerMaxEntriesInBatchTest extends ProducerConsumerBase {
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


    /**
     * in this test, entry size > consumer's permis > {@link org.apache.pulsar.broker.ServiceConfiguration#getDispatcherMaxRoundRobinBatchSize()}
     * so, dispatcherMaxRoundRobinBatchSize's limitation will reach first.
     *
     * @throws Exception
     */
    @Test
    public void testMaxEntriesInBatchWithDispatcherMaxRoundRobinBatchSizeSmallest() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        final int dispatcherMaxRoundRobinBatchSize = 20;
        conf.setDispatcherMaxRoundRobinBatchSize(dispatcherMaxRoundRobinBatchSize);
        final int messageSize = 200;
        final int consumerQueueSize = 200;
        final int batchSize = 5;

        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                        .receiverQueueSize(consumerQueueSize)
                        .subscriptionType(SubscriptionType.Shared).subscribe();

        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();

        Awaitility.await().atMost(30000, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(((PersistentDispatcherMultipleConsumers) dispatcher).getNextConsumer() != null);
        });
        for (int i = 1; i < messageSize; i++) {
            int maxEntriesInThisBatch = ((PersistentDispatcherMultipleConsumers) dispatcher).getMaxEntriesInThisBatch(i,
                    ((PersistentDispatcherMultipleConsumers) dispatcher).getNextConsumer(), batchSize);
            if (i / batchSize < conf.getDispatcherMaxRoundRobinBatchSize()) {
                assertEquals(maxEntriesInThisBatch, i % batchSize == 0 ? i / batchSize : i / batchSize + 1);
            } else {
                assertEquals(maxEntriesInThisBatch, conf.getDispatcherMaxRoundRobinBatchSize());
            }
        }

        consumer.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * in this test, entry size > {@link org.apache.pulsar.broker.ServiceConfiguration#getDispatcherMaxRoundRobinBatchSize()} > consumer' permits.
     * so, consumer's permits limitation will reach first.
     *
     * @throws Exception
     */
    @Test
    public void testMaxEntriesInBatchWithMessageRangeAndSmallestQueueSize() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        final int dispatcherMaxRoundRobinBatchSize = 20;
        conf.setDispatcherMaxRoundRobinBatchSize(dispatcherMaxRoundRobinBatchSize);
        final int messageSize = 200;
        final int consumerQueueSize = 75;
        final int batchSize = 5;

        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer =
                pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                        .receiverQueueSize(consumerQueueSize)
                        .subscriptionType(SubscriptionType.Shared).subscribe();

        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();

        Awaitility.await().atMost(30000, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(((PersistentDispatcherMultipleConsumers) dispatcher).getNextConsumer() != null);
        });
        for (int i = 1; i < messageSize; i++) {
            int maxEntriesInThisBatch = ((PersistentDispatcherMultipleConsumers) dispatcher).getMaxEntriesInThisBatch(i,
                    ((PersistentDispatcherMultipleConsumers) dispatcher).getNextConsumer(), batchSize);
            if (i / batchSize < consumerQueueSize / batchSize) {
                assertEquals(maxEntriesInThisBatch, i % batchSize == 0 ? i / batchSize : i / batchSize + 1);
            } else {
                assertEquals(maxEntriesInThisBatch, consumerQueueSize % batchSize == 0 ? consumerQueueSize / batchSize : consumerQueueSize / batchSize + 1);
            }
        }

        consumer.close();
        admin.topics().delete(topicName, false);
    }
}
