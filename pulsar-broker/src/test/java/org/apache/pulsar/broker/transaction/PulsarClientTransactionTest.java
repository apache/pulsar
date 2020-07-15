package org.apache.pulsar.broker.transaction;

import com.google.common.collect.Sets;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.PartitionedProducerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class PulsarClientTransactionTest extends BrokerTestBase {

    private static final String CLUSTER_NAME = "test";
    private static final String TENANT = "tnx";
    private static final String NAMESPACE1 = TENANT + "/ns1";
    private static final String TOPIC_INPUT_1 = NAMESPACE1 + "/input1";
    private static final String TOPIC_INPUT_2 = NAMESPACE1 + "/input2";
    private static final String TOPIC_OUTPUT_1 = NAMESPACE1 + "/output1";
    private static final String TOPIC_OUTPUT_2 = NAMESPACE1 + "/output2";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        init();

        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(brokerUrl.toString()));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE1);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_1, 3);
        admin.topics().createPartitionedTopic(TOPIC_INPUT_2, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_1, 3);
        admin.topics().createPartitionedTopic(TOPIC_OUTPUT_2, 3);

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        admin.topics().createPartitionedTopic(TopicName.TRANSACTION_COORDINATOR_ASSIGN.toString(), 16);

        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI(pulsar.getBrokerServiceUrl());
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);

        Thread.sleep(1000 * 3);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void produceTxnTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        PartitionedProducerImpl<byte[]> outProducer = (PartitionedProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(TOPIC_OUTPUT_1)
                .sendTimeout(0, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        List<CompletableFuture<MessageId>> messageIdFutureList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<MessageId> produceFuture = outProducer
                    .newMessage(tnx).value("Hello Txn!".getBytes(UTF_8)).sendAsync();
            messageIdFutureList.add(produceFuture);
        }


        tnx.commit().get();

        CountDownLatch countDownLatch = new CountDownLatch(5);
        messageIdFutureList.forEach(messageIdFuture -> {
            messageIdFuture.whenComplete((messageId, throwable) -> {
                countDownLatch.countDown();
                if (throwable != null) {
                    log.error("Tnx commit failed! tnx: " + tnx, throwable);
                    Assert.fail("Tnx commit failed! tnx: " + tnx);
                    return;
                }
                Assert.assertNotNull(messageId);
                log.info("Tnx commit success! messageId: {}", messageId);
            });
        });
        countDownLatch.await();
    }

    @Test
    public void clientTest() throws Exception {
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        Transaction tnx = pulsarClientImpl.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build().get();

        String input1 = NAMESPACE1 + "/input1";
        String input2 = NAMESPACE1 + "/input2";
        String output = NAMESPACE1 + "/output";

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClientImpl
                .newConsumer()
                .topic(input1)
                .subscriptionName("test")
                .subscribe();

        ConsumerImpl<byte[]> consumer2 = (ConsumerImpl<byte[]>) pulsarClientImpl
                .newConsumer()
                .topic(input2)
                .subscriptionName("test")
                .subscribe();

        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(input1)
                .create();

        producer1.newMessage().value("Hello Tnx1".getBytes()).send();

        ProducerImpl<byte[]> producer2 = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(input2)
                .create();

        producer2.newMessage().value("Hello Tnx1".getBytes()).send();

        Message<byte[]> inComingMsg1 = consumer1.receive();
        CompletableFuture<Void> ackFuture1 = consumer1.acknowledgeAsync(inComingMsg1.getMessageId(), tnx);

        Message<byte[]> inComingMsg2 = consumer2.receive();
        CompletableFuture<Void> ackFuture2 = consumer2.acknowledgeAsync(inComingMsg2.getMessageId(), tnx);

        ProducerImpl<byte[]> outProducer = (ProducerImpl<byte[]>) pulsarClientImpl
                .newProducer()
                .topic(output)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        CompletableFuture<MessageId> outFuture1 = outProducer
                .newMessage(tnx).value(inComingMsg1.getData()).sendAsync();

        CompletableFuture<MessageId> outFuture2 = outProducer
                .newMessage(tnx).value(inComingMsg2.getData()).sendAsync();

        tnx.abort().get();

        ackFuture1.whenComplete((i, t) -> log.info("finish ack1"));
        ackFuture2.whenComplete((i, t) -> log.info("finish ack2"));
        outFuture1.whenComplete((id, t) -> log.info("finish out1 msgId: {}", id));
        outFuture2.whenComplete((id, t) -> log.info("finish out2 msgId: {}", id));
    }

}
