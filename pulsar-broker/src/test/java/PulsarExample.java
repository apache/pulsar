import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Cleanup;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PulsarExample {

    private final String serviceUrl = "pulsar://localhost:6650";
    private final PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build();

    final String adminUrl = "http://localhost:8080";
    PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();

    private final String subscriptionName = "java-subscription-bb";

    private final String topicName = "my-topic-bbb";

    public PulsarExample() throws PulsarClientException {

    }

    @Test
    public void testSendSeqID() throws Exception {
        String topicName = "seqid3-test3-topic3";

        deleteTopic(topicName);

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .producerName("producer-without-txn")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        MessageId msgId1 = producer.newMessage().value("msg1".getBytes()).sequenceId(4).send();
        MessageId msgId2 = producer.newMessage().value("msg1".getBytes()).sequenceId(3).send();
        System.out.println(msgId1 + ", " + msgId2);
        producer.close();

    }

    @Test
    public void testTxnSendSeqID() throws Exception {
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true)
                .build();

        String topicName = "seqid2-test2-topic2";

        deleteTopic(topicName);

        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .producerName("my-test-producer")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();


        Transaction transaction = client.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();

//        MessageId msgId1 = producer.newMessage(transaction).value("msg1".getBytes()).send();
        MessageId msgId2 = producer.newMessage(transaction).value("msg2".getBytes()).sequenceId(4).send();

        transaction.commit();

        transaction = client.newTransaction()
                .withTransactionTimeout(10, TimeUnit.SECONDS)
                .build()
                .get();

        MessageId msgId3 = producer.newMessage(transaction).sequenceId(2).value("msg2".getBytes()).send();
//        producer.newMessage(transaction).value("msg2".getBytes()).send();
//        producer.newMessage(transaction).value("msg3".getBytes()).send();
//        producer.newMessage(transaction).value("msg4".getBytes()).send();
//        MessageId msgId4 = producer.newMessage(transaction).value("msg2".getBytes()).sequenceId(1).send();

        transaction.commit();


        deleteTopic(topicName);
    }

    @Test
    public void testSequenceIdInSyncCodeSegment() throws Exception {
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true)
                .build();

        String topicName = "seqid-test-topic";
        String subName = "seqid-test-sub";
        deleteTopic(topicName);

        //build producer/consumer
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .producerName("my-test-producer")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .consumerName("my-test-consumer")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(subName)
                .subscribe();

        for (int i = 0; i < 100; i++) {
            //send and ack messages with transaction
            Transaction transaction = client.newTransaction()
                    .withTransactionTimeout(10, TimeUnit.SECONDS)
                    .build()
                    .get();

            MessageId msgId1 = producer.newMessage(transaction).value("msg1".getBytes()).sequenceId(4).send();
            MessageId msgId2 = producer.newMessage(transaction).value("msg2".getBytes()).sequenceId(1).send();
//        MessageId msgId3 = producer.newMessage(transaction).value("msg3".getBytes()).sequenceId(2).send();
//        MessageId msgId4 = producer.newMessage(transaction).value("msg4".getBytes()).sequenceId(3).send();
            System.out.println("send success: " + msgId1);
            System.out.println("send success: " + msgId2);
//        System.out.println(msgId3);
//        System.out.println(msgId4);

            transaction.commit();

            Message<byte[]> message1 = consumer.receive(1, TimeUnit.SECONDS);
            Message<byte[]> message2 = consumer.receive(1, TimeUnit.SECONDS);

//        System.out.println(new String(message1.getData()));
//        System.out.println(new String(message2.getData()));
//        Message<byte[]> message3 = consumer.receive(1, TimeUnit.SECONDS);
//        Message<byte[]> message4 = consumer.receive(1, TimeUnit.SECONDS);
            System.out.println("receive success: " + message1.getMessageId() + " = " + new String(message1.getData()));
            System.out.println("receive success: " + message2.getMessageId() + " = " + new String(message2.getData()));

            consumer.acknowledge(message1);
            consumer.acknowledge(message2);

            Assert.assertEquals(message1.getMessageId(), msgId1);
            Assert.assertEquals(message2.getMessageId(), msgId2);
//        Assert.assertEquals(message3.getMessageId(), msgId3);
//        Assert.assertEquals(message4.getMessageId(), msgId4);
        }


        deleteTopic(topicName);
    }

    @Test
    public void testUpdateSequenceIdInSyncCodeSegment() throws Exception {
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTransaction(true)
                .build();

        String topic = "sequenceId";
        int totalMessage = 2;
        int threadSize = 5;
        String topicName = "subscription";
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);

        //build producer/consumer
        Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .producerName("producer")
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName(topicName)
                .subscribe();

        //send and ack messages with transaction
        Transaction transaction = client.newTransaction()
                .withTransactionTimeout(5, TimeUnit.SECONDS)
                .build()
                .get();

        for (int i = 0; i < totalMessage * threadSize; i++) {
            producer.newMessage().send();
        }


        producer.newMessage(transaction).value("msg1".getBytes()).sequenceId(1).send();
        producer.newMessage(transaction).value("msg3".getBytes()).sequenceId(3).send();
        producer.newMessage(transaction).value("msg2".getBytes()).sequenceId(2).send();
        producer.newMessage(transaction).value("msg4".getBytes()).sequenceId(4).send();

        CountDownLatch countDownLatch = new CountDownLatch(threadSize);
        for (int i = 0; i < threadSize; i++) {
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < totalMessage; j++) {
                        //The message will be sent with out-of-order sequence ID.
                        producer.newMessage(transaction).sendAsync();
                        Message<byte[]> message = consumer.receive();
                        consumer.acknowledgeAsync(message.getMessageId(), transaction);
                    }
                } catch (Exception e) {
                    System.out.println("Failed to send/ack messages with transaction." + e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        //wait the all send/ack op is executed and store its futures in the arraylist.
        countDownLatch.await(5, TimeUnit.SECONDS);
        //The transaction will be failed due to timeout.
        transaction.commit().get();
    }


    @Test
    public void testSchema4() throws PulsarClientException, PulsarAdminException {
        String topicName = RandomStringUtils.random(5);

        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
        recordSchemaBuilder.field("intField").type(SchemaType.INT32);
        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);

        Consumer<GenericRecord> consumer = client.newConsumer(Schema.generic(schemaInfo))
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();

        Producer<GenericRecord> producer = client.newProducer(Schema.generic(schemaInfo))
                .topic(topicName)
                .create();

        GenericSchemaImpl schema = GenericAvroSchema.of(schemaInfo);
        GenericRecord record = schema.newRecordBuilder().set("intField", 32).build();
        producer.newMessage().value(record).send();

        Message<GenericRecord> msg = consumer.receive();
        Assert.assertEquals(msg.getValue().getField("intField"), 32);
    }

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User2 {
        String name;
        int age;
    }

    @Test
    public void testSchema3() throws PulsarClientException, PulsarAdminException {
        String topicName = "ajdkf";
        Consumer<User2> consumer = client.newConsumer(Schema.AVRO(User2.class))
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscribe();
        Producer<User2> producer = client.newProducer(Schema.AVRO(User2.class))
                .topic(topicName)
                .create();

        producer.newMessage().value(User2.builder().name("pulsar-user").age(1).build()).send();

        User2 user = consumer.receive().getValue();
        Assert.assertEquals(user.name, "pulsar-user");
        Assert.assertEquals(user.age, 1);
    }

    @Test
    public void testSchema2() throws PulsarClientException, PulsarAdminException {
        String topicName = "user-topic2";
        deleteTopic(topicName);

        Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
                Schema.INT32,
                Schema.STRING,
                KeyValueEncodingType.INLINE
        );

        Producer<KeyValue<Integer, String>> producer = client.newProducer(kvSchema)
                .topic(topicName)
                .create();

        final int key = 100;
        final String value = "value-100";

        // send the key/value message
        producer.newMessage()
                .value(new KeyValue(key, value))
                .send();

        Consumer<KeyValue<Integer, String>> consumer = client.newConsumer(kvSchema)
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subscriptionName).subscribe();

        // receive key/value pair
        Message<KeyValue<Integer, String>> msg = consumer.receive();
        KeyValue<Integer, String> kv = msg.getValue();
        Assert.assertEquals((int) kv.getKey(), key);
        Assert.assertEquals(kv.getValue(), value);


//        deleteTopic(topicName);
    }


    static class User {
        public String name;
        public int age;

        User() {
        }

        User(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    @Test
    public void testSchema() throws PulsarClientException {
        String schemaTopic = "user-topic2";
        Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
                .topic(schemaTopic)
                .create();

        MessageId msgID = producer.send(new User("tom", 10));
        System.out.println("send success: " + msgID.toString());

        Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class))
                .topic(schemaTopic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("schema-sub")
                .subscribe();

        Message<User> message = consumer.receive(1, TimeUnit.SECONDS);
        User user = message.getValue();
        assert user.age == 10 && user.name.equals("tom");
    }


    @Test
    public void testLocalProducer() throws PulsarClientException, InterruptedException {
        Producer<byte[]> localProducer = client.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .create();

//        MessageId messageId1 = localProducer.send(("hello1").getBytes());
//        MessageId messageId2 = localProducer.send(("hello2").getBytes());
//        System.out.println(messageId1);
//        Thread.sleep(1 * 1000);

        for (int i = 0; i < 10; i++) {
            MessageId messageId = localProducer.send(("hello-" + i).getBytes());
            System.out.println(messageId.toString());
        }
    }


    @Test
    public void testReceive() throws PulsarClientException, InterruptedException {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//                .receiverQueueSize(0)
//                .priorityLevel()
                .subscribe();
        consumer.acknowledge(new MessageIdImpl(16, 5, -1));
//        consumer.acknowledge(new MessageIdImpl(16, 14, -1));
//        consumer.acknowledge(new MessageIdImpl(16, 8, -1));
//        consumer.acknowledge(new MessageIdImpl(16, 9, -1));
//        consumer.redeliverUnacknowledgedMessages();
//        for (int i = 0; i < 5; i++) {
//            Message<byte[]> message = consumer.receive();
//            System.out.println(message.getMessageId().toString() + "  " + new String(message.getData()));
//            consumer.acknowledge(message);
////            Thread.sleep(1000);
//        }
        consumer.close();
    }


    @Test
    public void testSendReceive() throws PulsarClientException {
        String testTopic = "my-test9";
        String msg = "hello";

        Producer<byte[]> producer = client.newProducer()
                .topic(testTopic)
                .enableBatching(false)
                .create();

        MessageId msgID = producer.send(msg.getBytes());
        System.out.println("send success: " + msgID.toString());

        Consumer<byte[]> consumer = client.newConsumer()
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .topic(testTopic)
                .subscriptionName("schema-sub")
                .subscribe();

        Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
        assert message.getMessageId().toString().equals(msgID.toString());
        assert msg.equals(new String(message.getData()));
    }

    @Test
    public void testTxnProducer()
            throws PulsarClientException, InterruptedException, ExecutionException, PulsarAdminException {
        String topicName = "test-tnx-topic-" + RandomStringUtils.random(3);
        String msg = "msg-in-txn";

        PulsarClient client = PulsarClient.builder()
                .enableTransaction(true)
                .serviceUrl(serviceUrl)
                .build();

        Transaction tnx = client.newTransaction().withTransactionTimeout(1, TimeUnit.DAYS).build().get();

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .create();

        int msgCount = 1000000;
        for (int i = 0; i < msgCount; i++) {
            CompletableFuture<MessageId> msgIdFuture = producer.newMessage(tnx).value((msg + i).getBytes()).sendAsync();
        }


        tnx.commit().get();
        System.out.println("committed: " + tnx.getTxnID().toString());
//        System.out.println("tnx send finish: " + msgIdFuture.get());

//        MessageId msgID2 = producer.send("without-tnx-msg".getBytes());
//        System.out.println("send finish: " + msgID2.toString());
        for (int i = 0; i < msgCount; i++) {
            Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
            Assert.assertEquals(new String(message.getData()), msg + i);
        }

//        System.out.println("receive: " + new String(message.getData()));

        deleteTopic(topicName);
    }

    private void deleteTopic(String topicName) throws PulsarAdminException {
        try {
            admin.topics().delete(topicName, true);
        } catch (PulsarAdminException.NotFoundException ignored) {

        }
    }

    @Test
    public void testReader() throws PulsarClientException, PulsarAdminException {
        Reader<byte[]> reader = client.newReader()
                .topic(topicName)
                .startMessageId(MessageId.latest)
                .startMessageIdInclusive()
                .create();
        if (reader.hasMessageAvailable()) {
            System.out.println(reader.readNext().getMessageId().toString());
        } else {
            System.out.println("nothing!");
        }
    }

    @Test
    public void testUnsubscribe() throws PulsarClientException, PulsarAdminException {
        Consumer<byte[]> consumer1 = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        consumer1.unsubscribe();

        Consumer<byte[]> consumer2 = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        consumer2.unsubscribe();

        List<String> subscriptions = admin.topics().getSubscriptions(topicName);
        Assert.assertEquals(subscriptions.size(), 1);
        Assert.assertEquals(subscriptions.get(0), subscriptionName);
    }


    @Test
    public void testDeleteSub() throws PulsarAdminException {
        admin.topics().deleteSubscription(topicName, "other-subscription");
    }


    @Test
    public void testCloudProduce() throws PulsarClientException, MalformedURLException {
        String issuerUrl = "https://auth.streamnative.cloud/";
        String credentialsUrl = "file:///Users/labuladong/Downloads/o-7udlj-free.json";
        String audience = "urn:sn:pulsar:o-7udlj:free";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("https://free.o-7udlj.aws-cnn1.streamnative.aws.snpulsar.cn")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(issuerUrl), new URL(credentialsUrl),
                                audience))
                .build();

        Producer<byte[]> producer = client.newProducer()
                .topic("topicName").create();

        for (int i = 0; i < 5; i++) {
            producer.send(("hello-" + i).getBytes());
        }

        client.close();
    }

    @Test
    public void testLocalExclusiveProducer() throws PulsarClientException, ExecutionException, InterruptedException {
        Producer<byte[]> producer1 = client.newProducer()
                .topic(topicName)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .enableChunking(false)
                .enableBatching(false)
                .create();

//        Producer<byte[]> producer2 = client.newProducer()
//                .topic(topicName)
//                .accessMode(ProducerAccessMode.WaitForExclusive)
//                .enableChunking(false)
//                .enableBatching(false)
//                .create();

        CompletableFuture<MessageId> msgIdFuture1 = producer1.sendAsync(("hello-1").getBytes());
//        msgIdFuture1.thenAccept((msgId) -> System.out.println(msgId.toString()));
//        CompletableFuture<MessageId> msgIdFuture2 = producer2.sendAsync(("hello-2").getBytes());
//        msgIdFuture2.thenAccept((msgId) -> System.out.println(msgId.toString()));
        System.out.println(msgIdFuture1.get().toString());
//        System.out.println(msgIdFuture2.get().toString());
    }

    @Test
    public void testReceiveLast() throws PulsarClientException {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        while (true) {
            Message<byte[]> message = consumer.receive();
            System.out.println(message.getMessageId().toString() + "  " + new String(message.getData()));
            consumer.acknowledge(message);
        }
    }

    @Test
    public void testFailoverSub() throws PulsarClientException {
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        while (true) {
            Message<byte[]> message = consumer.receive();
            System.out.println(message.getMessageId().toString() + "  " + new String(message.getData()));
            consumer.acknowledge(message);
        }

    }

    public static void main(String[] args) throws PulsarClientException {

    }
}