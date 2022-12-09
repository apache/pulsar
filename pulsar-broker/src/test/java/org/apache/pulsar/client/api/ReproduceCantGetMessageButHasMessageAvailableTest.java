package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/***
 * TODO discuss:
 *  plan-1: 发现读到 null 就 reset last message id in broker。这将导致 read timeout 被当作一种正常现象。
 *  plan-2: 增加 api: hasMessageAvailable(boolean fetchLastMessageIdFromBroker)
 *  plan-3: 增加 cmd，发生了 lastMessageId 变小的了情况，通知给 client。
 *  plan-4: last message id 记录在 broker.consumer. 用于改变 compaction 的行为。[重要的]
 *
 * TODO Discuss 2:
 *   retention 策略不是会把 cursor 往前移动吗？
 *   compaction 最后一条消息不执行 compaction
 *
 * TODO Enabled batch 后的 latLastMessageId 更新。
 *
 * TODO
 *   1.增加可选项：避免 trim 正在读取的 ledger 【重要的】
 *   2.compaction 的问题单独解决
 *   3.transaction buffer 单独解决
 *
 *   1: 决定留哪些 entry
 *   2：compaction cursor markDeleted 往前挪一下（ 如果有 reader 还有 race condition ）。
 *
 *   task running 参数：是否执行最后一条。
 *   reader
 *
 * This test is just to show that reader may not consume any messages even if "reader.hasMessageAvailable" returns true.
 */
public class ReproduceCantGetMessageButHasMessageAvailableTest extends ProducerConsumerBase {

    private static final String NAMESPACE = "public/default";

    @BeforeClass
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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Disable the scheduled task: compaction.
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(Integer.MAX_VALUE);
        // Disable the scheduled task: retention.
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
        // Messages can be held for a maximum of one minute.
        conf.setDefaultRetentionTimeInMinutes(120);
    }

    private String randomTopic(){
        return String.format("persistent://%s/%s", NAMESPACE, BrokerTestUtil.newUniqueName("tp"));
    }

    /**
     * Enabled compacted read and without batch sends, the last message has been deleted by compaction task.
     */
    @Test
    public void testRaceConditionWithCompactionDisabledBatch() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        List<Pair<String,String>> messagesToSend = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2"),
                Pair.of("k2", null),
                Pair.of("k3", "v3"),
                Pair.of("k3", null)
        );
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .create();

        sendMessages(topicName, false, messagesToSend, 1);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        reader.hasMessageAvailable();

        // The last message id of compacted topic will less than `managedLedger.lastConfirmedPosition`.
        triggerCompactionAndWait(topicName);

        verifyHasMessageAvailableButCantGet(reader);

        // cleanup.
        reader.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * Enabled compacted read and with batch sends, the last message has been deleted by compaction task.
     */
    @Test
    public void testRaceConditionWithCompactionEnabledBatch() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        List<Pair<String,String>> firstEntry = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2")
        );
        List<Pair<String,String>> secondEntry = Arrays.asList(
                Pair.of("k3", "v3"),
                Pair.of("k4", "v4"),
                Pair.of("k4", null)
        );
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .create();

        sendMessages(topicName, true, firstEntry, 1);
        sendMessages(topicName, true, secondEntry, 1);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        reader.hasMessageAvailable();

        // The last message id of compacted topic will equals `managedLedger.lastConfirmedPosition`. But the last
        // message in the last entry will be lost by consumer, because it has been marked `compactedOut`.
        triggerCompactionAndWait(topicName);

        verifyHasMessageAvailableButCantGet(reader);

        // cleanup.
        reader.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * Enabled compacted read and with batch sends, the last message read from compacted topic and marked "compactedOut"
     * has been lost by consumer.
     */
    @Test
    public void testReadAfterCompactionEnabledBatch() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        List<Pair<String,String>> messagesToSend = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2"),
                Pair.of("k2", null),
                Pair.of("k3", "v3"),
                Pair.of("k3", null)
        );
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(true)
                .create();

        sendMessages(topicName, true, messagesToSend, 3);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        triggerCompactionAndWait(topicName);

        // The variable `lastMessageIdInBroker` of consumer will be the last message id of compacted topic. But the last
        // message in the last entry will be lost by consumer, because it has been marked `compactedOut`.
        verifyHasMessageAvailableButCantGet(reader);

        // cleanup.
        reader.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * No durable cursor exists, all messages deleted by trim ledgers task.
     */
    @Test
    public void testRaceConditionWithRetentionAndNoDurableCursorExists() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        List<Pair<String,String>> messagesToSend = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2"),
                Pair.of("k3", "v3"),
                Pair.of("k4", "v4")
        );
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(false)
                .create();

        sendMessages(topicName, false, messagesToSend, 1);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        reader.hasMessageAvailable();

        triggerLedgerSwitch(topicName);
        clearAllTheLedgersOutdated(topicName);

        verifyHasMessageAvailableButCantGet(reader);

        // cleanup.
        reader.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * There are durable cursors, and durable cursors have been consumed to the end. The last non-empty ledger has been
     * deleted when opening managed ledger.
     */
    @Test
    public void testRaceConditionWithForwardCursorWhenOpenManagedLedger() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        String subNameDurable = "sub_durable";
        List<Pair<String,String>> messagesToSend = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2"),
                Pair.of("k3", "v3"),
                Pair.of("k4", "v4")
        );
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .consumerName("c_reproduce_issue")
                .subscriptionName(subNameDurable)
                .receiverQueueSize(1000)
                .readCompacted(false)
                .subscribe();
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(false)
                .create();

        sendMessages(topicName, false, messagesToSend, 1);

        ackAllMessages(consumer);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        reader.hasMessageAvailable();

        triggerLedgerSwitch(topicName);
        clearAllTheLedgersOutdated(topicName);

        verifyHasMessageAvailableButCantGet(reader);

        // cleanup.
        consumer.close();
        reader.close();
        admin.topics().delete(topicName, false);
    }

    /**
     * There are durable cursors, and durable cursors have been consumed to the end. If the managed ledger is not closed
     * and then reopened, the last non-empty ledger will not be deleted.
     * This test is used only to demonstrate the existence of a phenomenon, and has no other purpose.
     */
    @Test(timeOut = 180 * 1000)
    public void testRaceConditionWithRetentionAndOneDurableCursorExists() throws Exception {
        String topicName = randomTopic();
        String subName = "sub_no_durable";
        String subNameDurable = "sub_durable";
        List<Pair<String,String>> messagesToSend = Arrays.asList(
                Pair.of("k1", "v1"),
                Pair.of("k2", "v2"),
                Pair.of("k3", "v3"),
                Pair.of("k4", "v4")
        );
        ConsumerImpl<String> consumer = (ConsumerImpl<String>) pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .consumerName("c_reproduce_issue")
                .subscriptionName(subNameDurable)
                .receiverQueueSize(1000)
                .readCompacted(false)
                .subscribe();
        Reader<String> reader = pulsarClient.newReader(Schema.STRING)
                .topic(topicName)
                .readerName("reproduce_issue")
                .startMessageId(MessageId.earliest)
                .subscriptionName(subName)
                .receiverQueueSize(1)
                .readCompacted(false)
                .create();

        sendMessages(topicName, false, messagesToSend, 1);

        // The variable `lastMessageIdInBroker` of consumer will be `managedLedger.lastConfirmedPosition`.
        reader.hasMessageAvailable();

        triggerLedgerSwitch(topicName);

        ackAllMessages(consumer);

        setRetentionTimeMillis(topicName, 1);
        try {
            clearAllTheLedgersOutdated(topicName);
            fail("Confirm that the retention policy does not delete the last non-empty ledger, even if it has been"
                    + " consumed");
        } catch (Exception ex){
            // ignore.
        }

        // cleanup.
        consumer.close();
        reader.close();
        admin.topics().delete(topicName, false);
    }

    private void setRetentionTimeMillis(String topicName, int RetentionTimeMillis) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        managedLedger.getConfig().setRetentionTime(RetentionTimeMillis, TimeUnit.MILLISECONDS);
    }

    private void ackAllMessages(ConsumerImpl<String> consumer) throws Exception {
        Message<String> message = null;
        while ((message = consumer.receive(2, TimeUnit.SECONDS)) != null){
            consumer.acknowledgeCumulative(message);
        }
    }

    private void triggerLedgerSwitch(String topicName) throws Exception{
        admin.topics().unload(topicName);
        Awaitility.await().until(() -> {
            CompletableFuture<Optional<Topic>> topicFuture =
                    pulsar.getBrokerService().getTopic(topicName, false);
            if (!topicFuture.isDone() || topicFuture.isCompletedExceptionally()){
                return false;
            }
            Optional<Topic> topicOptional = topicFuture.join();
            if (!topicOptional.isPresent()){
                return false;
            }
            PersistentTopic persistentTopic = (PersistentTopic) topicOptional.get();
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            return managedLedger.getState() == ManagedLedgerImpl.State.LedgerOpened;
        });
    }

    private void clearAllTheLedgersOutdated(String topicName) throws Exception{
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            CompletableFuture future = new CompletableFuture();
            managedLedger.trimConsumedLedgersInBackground(future);
            future.join();
            return managedLedger.getLedgersInfo().size() == 1;
        });
    }

    private void verifyHasMessageAvailableButCantGet(Reader<String> reader) throws Exception {
        boolean receiveNullEvenIfHasMessageAvailable = false;
        while (reader.hasMessageAvailable()) {
            Message message = reader.readNext(2, TimeUnit.SECONDS);
            if (message == null) {
                receiveNullEvenIfHasMessageAvailable = true;
                break;
            }
        }
        assertTrue(receiveNullEvenIfHasMessageAvailable, "If this test fails, you need to modify the doc for"
                + " method hasMessageAvailable。");
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();

        Awaitility.await().untilAsserted(() -> {
            PositionImpl lastConfirmPos = (PositionImpl) persistentTopic.getManagedLedger().getLastConfirmedEntry();
            PositionImpl markDeletePos = (PositionImpl) persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }

    private void sendMessages(String topicName, boolean enabledBatch, List<Pair<String,String>> messagesToSend,
                              int sendMessagesLoopCount) throws Exception {
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(enabledBatch)
                .create();

        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>();
        for (int i = 0; i < sendMessagesLoopCount; i++) {
            for (Pair<String, String> messageToSend : messagesToSend) {
                String key = messageToSend.getLeft();
                String value = messageToSend.getRight();
                if (key == null) {
                    sendFutures.add(producer.newMessage().value(value).sendAsync());
                } else {
                    sendFutures.add(producer.newMessage().key(key).value(value).sendAsync());
                }
            }
            producer.flush();
        }
        FutureUtil.waitForAll(sendFutures).join();
        producer.close();
    }
}