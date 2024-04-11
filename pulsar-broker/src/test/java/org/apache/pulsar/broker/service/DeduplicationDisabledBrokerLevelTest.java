package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeduplicationDisabledBrokerLevelTest extends ProducerConsumerBase {

    private final int deduplicationSnapshotFrequency = 5;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        this.conf.setBrokerDeduplicationEnabled(false);
        this.conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(deduplicationSnapshotFrequency);
    }

    @Test
    public void testNoBacklogOnDeduplication() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
        final PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false).join().get();
        final ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        // deduplication enabled:
        //   broker level: "false"
        //   topic level: "true".
        // So it is enabled.
        admin.topicPolicies().setDeduplicationStatus(topic, true);
        Awaitility.await().untilAsserted(() -> {
            ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
            assertNotNull(cursor);
        });

        // Verify: regarding deduplication cursor, messages will be acknowledged automatically.
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        producer.send("1");
        producer.send("2");
        producer.send("3");
        producer.close();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get(PersistentTopic.DEDUPLICATION_CURSOR_NAME);
        Awaitility.await().atMost(Duration.ofSeconds(deduplicationSnapshotFrequency * 3)).untilAsserted(() -> {
            PositionImpl LAC = (PositionImpl) ml.getLastConfirmedEntry();
            PositionImpl cursorMD = (PositionImpl) cursor.getMarkDeletedPosition();
            assertTrue(LAC.compareTo(cursorMD) <= 0);
        });

        // cleanup.
        admin.topics().delete(topic);
    }
}
