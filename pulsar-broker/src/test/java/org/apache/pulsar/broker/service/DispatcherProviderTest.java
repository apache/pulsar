package org.apache.pulsar.broker.service;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class DispatcherProviderTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setDefaultNumberOfNamespaceBundles(1);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
        conf.setDispatcherProviderClassName("org.apache.pulsar.broker.service.DispatcherProvider.DefaultDispatcherProvider");
    }

    @Test
    public void testDispatcherProviderImpl() throws Exception {
        cleanup();
        conf.setDispatcherProviderClassName("org.apache.pulsar.broker.service.DispatcherProviderTest$MockDispatcherProvider");
        setup();

        String topic = "persistent://my-property/my-ns/topic" + UUID.randomUUID();
        String subName = "sub";
        admin.topics().createNonPartitionedTopic(topic);
        pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .subscribe().close();
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get();
        PersistentSubscription subscription = persistentTopic.getSubscription(subName);
        Dispatcher dispatcher = subscription.getDispatcher();

        assertEquals(dispatcher.getType(), CommandSubscribe.SubType.Key_Shared);
        assertEquals(dispatcher.getConsumers().size(), 1);

    }

    @Getter
    public static class MockDispatcherProvider implements DispatcherProvider {

        @Override
        public Dispatcher createDispatcher(Consumer consumer, Subscription subscription) {
            return new Dispatcher() {
                @Override
                public void addConsumer(Consumer consumer) throws BrokerServiceException {
                }

                @Override
                public void removeConsumer(Consumer consumer) throws BrokerServiceException {
                }

                @Override
                public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

                }

                @Override
                public boolean isConsumerConnected() {
                    return false;
                }

                @Override
                public List<Consumer> getConsumers() {
                    List<Consumer> consumers = new ArrayList<>();
                    consumers.add(mock(Consumer.class));
                    return consumers;
                }

                @Override
                public boolean canUnsubscribe(Consumer consumer) {
                    return false;
                }

                @Override
                public CompletableFuture<Void> close() {
                    return null;
                }

                @Override
                public boolean isClosed() {
                    return false;
                }

                @Override
                public CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
                    return null;
                }

                @Override
                public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
                    return null;
                }

                @Override
                public void resetCloseFuture() {

                }

                @Override
                public void reset() {

                }

                @Override
                public CommandSubscribe.SubType getType() {
                    return CommandSubscribe.SubType.Key_Shared;
                }

                @Override
                public void redeliverUnacknowledgedMessages(Consumer consumer) {

                }

                @Override
                public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {

                }

                @Override
                public void addUnAckedMessages(int unAckMessages) {

                }

                @Override
                public RedeliveryTracker getRedeliveryTracker() {
                    return null;
                }
            };
        }
    }


}
