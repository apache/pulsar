package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import lombok.Cleanup;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReplicatedSubscriptionConfigTest extends ProducerConsumerBase {
    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void createReplicatedSubscription() throws Exception {
        String topic = "createReplicatedSubscription-" + System.nanoTime();

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .replicateSubscriptionState(true)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertTrue(stats.subscriptions.get("sub1").isReplicated);

        admin.topics().unload(topic);

        // Check that subscription is still marked replicated after reloading
        stats = admin.topics().getStats(topic);
        assertTrue(stats.subscriptions.get("sub1").isReplicated);
    }

    @Test
    public void upgradeToReplicatedSubscription() throws Exception {
        String topic = "upgradeToReplicatedSubscription-" + System.nanoTime();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.subscriptions.get("sub").isReplicated);
        consumer.close();

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.subscriptions.get("sub").isReplicated);
        consumer.close();
    }

    @Test
    public void upgradeToReplicatedSubscriptionAfterRestart() throws Exception {
        String topic = "upgradeToReplicatedSubscriptionAfterRestart-" + System.nanoTime();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.subscriptions.get("sub").isReplicated);
        consumer.close();

        admin.topics().unload(topic);

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.subscriptions.get("sub").isReplicated);
        consumer.close();
    }
}
