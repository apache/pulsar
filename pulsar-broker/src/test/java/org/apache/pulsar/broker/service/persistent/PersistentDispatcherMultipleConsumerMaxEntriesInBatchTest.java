package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumerMaxEntriesInBatchTest {

    /**
     * in this test, remainingMessages > consumer's permits >
     * {@link org.apache.pulsar.broker.ServiceConfiguration#getDispatcherMaxRoundRobinBatchSize()}
     * so, dispatcherMaxRoundRobinBatchSize's limitation will reach first.
     */
    @Test
    public void testMaxEntriesInBatchWithDispatcherMaxRoundRobinBatchSizeSmallest() {
        final int dispatcherMaxRoundRobinBatchSize = 20;
        final int remainingMessages = 200;
        final int availablePermits = 200;
        final int avgBatchSizePerMsg = 5;
        final int maxUnackedMessages = 50000;
        final int unackedMessages = 0;

        for (int i = 1; i < remainingMessages; i++) {
            int maxEntriesInThisBatch =
                    PersistentDispatcherMultipleConsumers.getMaxEntriesInThisBatch(i, maxUnackedMessages,
                            unackedMessages,
                            avgBatchSizePerMsg, availablePermits, dispatcherMaxRoundRobinBatchSize);
            if (i / avgBatchSizePerMsg < dispatcherMaxRoundRobinBatchSize) {
                assertEquals(maxEntriesInThisBatch,
                        i % avgBatchSizePerMsg == 0 ? i / avgBatchSizePerMsg : i / avgBatchSizePerMsg + 1);
            } else {
                assertEquals(maxEntriesInThisBatch, dispatcherMaxRoundRobinBatchSize);
            }
        }
    }

    /**
     * in this test, remainingMessages >
     * {@link org.apache.pulsar.broker.ServiceConfiguration#getDispatcherMaxRoundRobinBatchSize()} > consumer' permits.
     * so, consumer's permits limitation will reach first.
     */
    @Test
    public void testMaxEntriesInBatchWithMessageRangeAndSmallestQueueSize() {
        final int dispatcherMaxRoundRobinBatchSize = 20;
        final int remainingMessages = 200;
        final int availablePermits = 75;
        final int avgBatchSizePerMsg = 5;
        final int maxUnackedMessages = 50000;
        final int unackedMessages = 0;

        for (int i = 1; i < remainingMessages; i++) {
            int maxEntriesInThisBatch =
                    PersistentDispatcherMultipleConsumers.getMaxEntriesInThisBatch(i, maxUnackedMessages,
                            unackedMessages,
                            avgBatchSizePerMsg, availablePermits, dispatcherMaxRoundRobinBatchSize);
            if (i < availablePermits) {
                assertEquals(maxEntriesInThisBatch,
                        i % avgBatchSizePerMsg == 0 ? i / avgBatchSizePerMsg : i / avgBatchSizePerMsg + 1);
            } else {
                assertEquals(maxEntriesInThisBatch,
                        availablePermits % avgBatchSizePerMsg == 0 ? availablePermits / avgBatchSizePerMsg :
                                availablePermits / avgBatchSizePerMsg + 1);
            }
        }
    }

    /**
     * in this test, entry size >
     * {@link org.apache.pulsar.broker.ServiceConfiguration#getDispatcherMaxRoundRobinBatchSize()} > consumer' permits >
     * unAckedMessages
     * so, unAckedMessages limitation will reach first.
     */
    @Test
    public void testMaxEntriesInBatchWithUnackedMessagesLimitation() {
        final int dispatcherMaxRoundRobinBatchSize = 20;
        final int remainingMessages = 200;
        final int availablePermits = 75;
        final int avgBatchSizePerMsg = 5;
        final int maxUnackedMessages = 500;
        final int unackedMessages = 480;

        for (int i = 1; i < remainingMessages; i++) {
            int maxEntriesInThisBatch =
                    PersistentDispatcherMultipleConsumers.getMaxEntriesInThisBatch(i, maxUnackedMessages,
                            unackedMessages,
                            avgBatchSizePerMsg, availablePermits, dispatcherMaxRoundRobinBatchSize);
            if (i < (maxUnackedMessages - unackedMessages)) {
                assertEquals(maxEntriesInThisBatch,
                        i % avgBatchSizePerMsg == 0 ? i / avgBatchSizePerMsg : i / avgBatchSizePerMsg + 1);
            } else {
                assertEquals(maxEntriesInThisBatch, (maxUnackedMessages - unackedMessages) / avgBatchSizePerMsg);
            }
        }
    }
}
