package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.pulsar.broker.rest.entity.AckMessageRequest;
import org.apache.pulsar.broker.rest.entity.CreateConsumerRequest;
import org.apache.pulsar.broker.rest.entity.CreateConsumerResponse;
import org.apache.pulsar.broker.rest.entity.GetMessagesResponse;
import org.apache.pulsar.broker.rest.entity.RestAckPosition;
import org.apache.pulsar.broker.rest.entity.RestAckType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class RestConsumerTest extends ProducerConsumerBase {
    private static final OkHttpClient client = new OkHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testCreateConsumer() throws IOException, JsonUtil.ParseJsonException, PulsarAdminException {
        String topicName = "persistent://public/default/producer-consumer-topic";
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        createConsumer("persistent", "public", "default",
                "producer-consumer-topic", consumerName, subscriptionName);
        List<? extends ConsumerStats> consumers = admin.topics().getStats(topicName)
                .getSubscriptions()
                .get(subscriptionName)
                .getConsumers();
        Assert.assertNotNull(consumers);
        Assert.assertTrue(consumers.stream().anyMatch(status -> Objects.equals(status.getConsumerName(), consumerName)));
    }

    @Test
    public void testCreatePartitionedTopicConsumer() throws IOException, JsonUtil.ParseJsonException, PulsarAdminException {
        String topicName = "persistent://public/default/producer-consumer-topic";
        admin.topics().createPartitionedTopic(topicName, 10);
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        createConsumer("persistent", "public", "default",
                "producer-consumer-topic", consumerName, subscriptionName);
        PartitionedTopicStats partitionedStats = admin.topics().getPartitionedStats(topicName, true);
        List<? extends ConsumerStats> consumers = partitionedStats.getPartitions().values()
                .stream()
                .flatMap(v -> v.getSubscriptions().values().stream()
                        .flatMap(s -> s.getConsumers().stream()))
                .collect(Collectors.toList());
        Assert.assertEquals(consumers.size(), 10);
        for (ConsumerStats consumer : consumers) {
            Assert.assertEquals(consumer.getConsumerName(), consumerName);
        }
    }

    private CreateConsumerResponse createConsumer(String domain, String tenant, String ns, String topic, String consumerName,
                                                  String subscriptionName) throws JsonUtil.ParseJsonException, IOException {
        String serviceUrl = admin.getServiceUrl();
        String createConsumer = serviceUrl +
                String.format("/topics/%s/%s/%s/%s/subscription/%s", domain, tenant, ns, topic, subscriptionName);
        CreateConsumerRequest createConsumerRequest = new CreateConsumerRequest();
        createConsumerRequest.setConsumerName(consumerName);
        RequestBody requestBody =
                RequestBody.create(JsonUtil.toJson(createConsumerRequest), MediaType.get("application/json"));
        final Request request = new Request.Builder()
                .url(createConsumer)
                .post(requestBody)
                .build();
        Call call = client.newCall(request);
        @Cleanup
        Response res = call.execute();
        Assert.assertEquals(200, res.code());
        String body = res.body().string();
        return JsonUtil.fromJson(body, CreateConsumerResponse.class);
    }

    private void deleteConsumer(String domain, String tenant, String ns, String topic,
                                String subscriptionName, String consumerId) throws JsonUtil.ParseJsonException, IOException {
        String serviceUrl = admin.getServiceUrl();
        String url = serviceUrl +
                String.format("/topics/%s/%s/%s/%s/subscription/%s/consumers/%s", domain, tenant, ns,
                        topic, subscriptionName, consumerId);
        final Request request = new Request.Builder()
                .url(url)
                .delete()
                .build();
        Call call = client.newCall(request);
        @Cleanup
        Response res = call.execute();
        Assert.assertEquals(200, res.code());
    }


    @Test
    public void testDeletePartitionedConsumer() throws JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        PartitionedTopicStats partitionedStats = admin.topics().getPartitionedStats(topicName, true);
        List<? extends ConsumerStats> consumerStatsList = partitionedStats.getPartitions().values()
                .stream()
                .flatMap(v -> v.getSubscriptions().values().stream()
                        .flatMap(s -> s.getConsumers().stream()))
                .collect(Collectors.toList());
        Assert.assertEquals(consumerStatsList.size(), 10);
        for (ConsumerStats consumerStats : consumerStatsList) {
            Assert.assertEquals(consumerStats.getConsumerName(), consumerName);
        }
        deleteConsumer("persistent", "public", "default",
                topicRandom, subscriptionName, consumer.getId());
        PartitionedTopicStats partitionedStats2 = admin.topics().getPartitionedStats(topicName, true);
        List<? extends ConsumerStats> consumerStatsList2 = partitionedStats2.getPartitions().values()
                .stream()
                .flatMap(v -> v.getSubscriptions().values().stream()
                        .flatMap(s -> s.getConsumers().stream()))
                .collect(Collectors.toList());
        Assert.assertEquals(consumerStatsList2.size(), 0);
    }

    @Test
    public void testDeleteConsumer() throws JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicName = "persistent://public/default/producer-consumer-test";
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                "producer-consumer-test", consumerName, subscriptionName);
        List<? extends ConsumerStats> consumers1 = admin.topics().getStats(topicName)
                .getSubscriptions()
                .get(subscriptionName)
                .getConsumers();
        Assert.assertNotNull(consumers1);
        Assert.assertTrue(consumers1.stream().anyMatch(status -> Objects.equals(status.getConsumerName(), consumerName)));
        deleteConsumer("persistent", "public", "default",
                "producer-consumer-test", subscriptionName, consumer.getId());
        List<? extends ConsumerStats> consumers2 = admin.topics().getStats(topicName)
                .getSubscriptions()
                .get(subscriptionName)
                .getConsumers();
        Assert.assertNotNull(consumers2);
        Assert.assertFalse(consumers2.stream().anyMatch(status -> Objects.equals(status.getConsumerName(), consumerName)));
    }

    private List<GetMessagesResponse> receiveMessage(String domain, String tenant, String ns,
                                                     String topic,
                                                     String subscriptionName,
                                                     String consumerId, int maxMessages, int timeout, long maxBytes)
            throws IOException {
        String serviceUrl = admin.getServiceUrl();
        String url = serviceUrl +
                String.format("/topics/%s/%s/%s/%s/subscription/%s/consumer/%s/messages", domain,
                        tenant, ns, topic, subscriptionName, consumerId);
        String finalUrl = url + String.format("?timeout=%s&maxMessage=%s&maxByte=%s", timeout, maxMessages, maxBytes);
        final Request request = new Request.Builder()
                .url(finalUrl)
                .get()
                .build();
        Call call = client.newCall(request);
        @Cleanup
        Response res = call.execute();
        Assert.assertEquals(200, res.code());
        return mapper.readValue(res.body().string(), new TypeReference<>() {});
    }

    private List<RestAckPosition> ackMessages(String domain, String tenant, String ns,
                                                 String topic, String subscriptionName,
                                                 String consumerId, List<RestAckPosition> restAckPositions,
                                                 RestAckType ackType)
            throws IOException, JsonUtil.ParseJsonException {
        String serviceUrl = admin.getServiceUrl();
        String url = serviceUrl +
                String.format("/topics/%s/%s/%s/%s/subscription/%s/consumer/%s/cursor", domain,
                        tenant, ns, topic, subscriptionName, consumerId);
        AckMessageRequest ackMessageRequest = new AckMessageRequest();
        ackMessageRequest.setAckType(ackType);
        ackMessageRequest.setMessagePositions(restAckPositions);
        RequestBody requestBody =
                RequestBody.create(JsonUtil.toJson(ackMessageRequest), MediaType.get("application/json"));
        final Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();
        Call call = client.newCall(request);
        @Cleanup
        Response res = call.execute();
        Assert.assertEquals(200, res.code());
        return mapper.readValue(res.body().string(), new TypeReference<>() {});
    }

    @Test
    public void testFetchMessagesFromNonPartitionedTopicStringSchema() throws JsonUtil.ParseJsonException, IOException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            String content = UUID.randomUUID().toString();
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(restMessageEntities.get(0).getValue(), content);
        }
    }

    @Test
    public void testFetchMessagesFromPartitionedTopicStringSchema() throws
            JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            String content = UUID.randomUUID().toString();
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(restMessageEntities.get(0).getValue(), content);
        }
    }

    @Test
    public void testFetchMessagesFromNonPartitionedTopicBytesSchema() throws JsonUtil.ParseJsonException, IOException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(Base64.getDecoder().decode(restMessageEntities.get(0).getValue()), content);
        }
    }

    @Test
    public void testFetchMessagesFromPartitionedTopicBytesSchema() throws
            JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            byte[] content = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(Base64.getDecoder().decode(restMessageEntities.get(0).getValue()), content);
        }
    }

    @Test
    public void testFetchMessagesFromNonPartitionedTopicJsonSchema() throws JsonUtil.ParseJsonException, IOException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.JSON(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
        }
    }

    @Test
    public void testFetchMessagesFromPartitionedTopicJsonSchema()
            throws JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.JSON(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
        }
    }


    @Test
    public void testFetchMessagesFromNonPartitionedTopicAvroSchema() throws JsonUtil.ParseJsonException, IOException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.AVRO(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
        }
    }

    @Test
    public void testFetchMessagesFromPartitionedTopicAvroSchema()
            throws JsonUtil.ParseJsonException, IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.AVRO(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
        }
    }

    @Test
    public void testAckForNonPartitionedTopic() throws JsonUtil.ParseJsonException, IOException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.AVRO(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
            List<RestAckPosition> msgIds = restMessageEntities
                    .stream()
                    .map(v -> new RestAckPosition(v.getLedgerId(), v.getEntryId(), v.getPartition()))
                    .collect(Collectors.toList());
            List<RestAckPosition> ackMessageResponses  =
                    ackMessages("persistent", "public", "default",
                            topicRandom, subscriptionName, id, msgIds, RestAckType.SINGLE);
            Assert.assertEquals(ackMessageResponses.size(), msgIds.size());
        }
        Awaitility.await().untilAsserted(() -> {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
            ManagedLedgerInternalStats.CursorStats cursorStats = stats.cursors.get(subscriptionName);
            Assert.assertNotNull(cursorStats);
            Assert.assertEquals(cursorStats.markDeletePosition, stats.lastConfirmedEntry);
        });
    }

    @Test
    public void testAckForPartitionedTopic() throws JsonUtil.ParseJsonException,
            IOException, PulsarAdminException {
        String topicRandom = "producer-consumer-test-" + UUID.randomUUID();
        String topicName = "persistent://public/default/" + topicRandom;
        String consumerName = "test-consumer";
        String subscriptionName = "test-sub";
        admin.topics().createPartitionedTopic(topicName, 10);
        CreateConsumerResponse consumer = createConsumer("persistent", "public", "default",
                topicRandom, consumerName, subscriptionName);
        String id = consumer.getId();
        Producer<Student> producer = pulsarClient.newProducer(Schema.AVRO(Student.class))
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 100; i++) {
            Student content = new Student("abc", 123);
            producer.send(content);
            List<GetMessagesResponse> restMessageEntities =
                    receiveMessage("persistent", "public", "default",
                            topicRandom, subscriptionName, id, 1, 1000, 99999);
            Assert.assertEquals(restMessageEntities.size(), 1);
            Assert.assertEquals(mapper.readValue(restMessageEntities.get(0).getValue(), Student.class), content);
            List<RestAckPosition> msgIds = restMessageEntities
                    .stream()
                    .map(v -> new RestAckPosition(v.getLedgerId(), v.getEntryId(), v.getPartition()))
                    .collect(Collectors.toList());
            List<RestAckPosition> ackMessageResponses =
                    ackMessages("persistent", "public", "default",
                            topicRandom, subscriptionName, id, msgIds, RestAckType.SINGLE);
            Assert.assertEquals(ackMessageResponses.size(), msgIds.size());
        }
        Awaitility.await().untilAsserted(() -> {
            PartitionedTopicInternalStats partitionedInternalStats = admin.topics().getPartitionedInternalStats(topicName);
            for (PersistentTopicInternalStats internalStats : partitionedInternalStats.partitions.values()) {
                String lastConfirmedEntry = internalStats.lastConfirmedEntry;
                Assert.assertEquals(internalStats.cursors.size(), 1);
                ManagedLedgerInternalStats.CursorStats cursorStats = internalStats.cursors.get(subscriptionName);
                Assert.assertEquals(lastConfirmedEntry, cursorStats.markDeletePosition);
            }
        });
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class Student {
        private String name;
        private int age;
    }
}
