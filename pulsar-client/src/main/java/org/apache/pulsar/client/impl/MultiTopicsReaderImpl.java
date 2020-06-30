package org.apache.pulsar.client.impl;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

public class MultiTopicsReaderImpl<T> implements Reader<T> {

    private final MultiTopicsConsumerImpl<T> consumer;

    public MultiTopicsReaderImpl(PulsarClientImpl client, ReaderConfigurationData<T> readerConfiguration,
                                 ExecutorService listenerExecutor, CompletableFuture<Consumer<T>> consumerFuture, Schema<T> schema) {
        String subscription = "multiTopicsReader-" + DigestUtils.sha1Hex(UUID.randomUUID().toString()).substring(0, 10);
        if (StringUtils.isNotBlank(readerConfiguration.getSubscriptionRolePrefix())) {
            subscription = readerConfiguration.getSubscriptionRolePrefix() + "-" + subscription;
        }
        ConsumerConfigurationData<T> consumerConfiguration = new ConsumerConfigurationData<>();
        consumerConfiguration.getTopicNames().add(readerConfiguration.getTopicName());
        consumerConfiguration.setSubscriptionName(subscription);
        consumerConfiguration.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfiguration.setSubscriptionMode(SubscriptionMode.NonDurable);
        consumerConfiguration.setReceiverQueueSize(readerConfiguration.getReceiverQueueSize());
        consumerConfiguration.setReadCompacted(readerConfiguration.isReadCompacted());

        if (readerConfiguration.getReaderListener() != null) {
            ReaderListener<T> readerListener = readerConfiguration.getReaderListener();
            consumerConfiguration.setMessageListener(new MessageListener<T>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void received(Consumer<T> consumer, Message<T> msg) {
                    readerListener.received(MultiTopicsReaderImpl.this, msg);
                    consumer.acknowledgeCumulativeAsync(msg);
                }

                @Override
                public void reachedEndOfTopic(Consumer<T> consumer) {
                    readerListener.reachedEndOfTopic(MultiTopicsReaderImpl.this);
                }
            });
        }

        if (readerConfiguration.getReaderName() != null) {
            consumerConfiguration.setConsumerName(readerConfiguration.getReaderName());
        }
        if (readerConfiguration.isResetIncludeHead()) {
            consumerConfiguration.setResetIncludeHead(true);
        }
        consumerConfiguration.setCryptoFailureAction(readerConfiguration.getCryptoFailureAction());
        if (readerConfiguration.getCryptoKeyReader() != null) {
            consumerConfiguration.setCryptoKeyReader(readerConfiguration.getCryptoKeyReader());
        }
        if (readerConfiguration.getKeyHashRanges() != null) {
            consumerConfiguration.setKeySharedPolicy(
                    KeySharedPolicy
                            .stickyHashRange()
                            .ranges(readerConfiguration.getKeyHashRanges())
            );
        }
        consumer = new MultiTopicsConsumerImpl<>(client, consumerConfiguration, listenerExecutor, consumerFuture, schema,
                null, true, readerConfiguration.getStartMessageId(), readerConfiguration.getStartMessageFromRollbackDurationInSec());
    }

    public static boolean isIllegalMultiTopicsReaderMessageId(MessageId messageId) {
        if (MessageId.earliest.equals(messageId) || MessageId.latest.equals(messageId)) {
            return false;
        }
        MessageIdImpl messageIdImpl = MessageIdImpl.convertToMessageIdImpl(messageId);
        if (messageIdImpl != null && messageIdImpl.getPartitionIndex() >= 0 && messageIdImpl.getLedgerId() >= 0
                && messageIdImpl.getEntryId() >= 0) {
            return false;
        }
        return true;
    }

    @Override
    public String getTopic() {
        return consumer.getTopic();
    }

    @Override
    public Message<T> readNext() throws PulsarClientException {
        Message<T> msg = consumer.receive();
        // Acknowledge message immediately because the reader is based on non-durable subscription. When it reconnects,
        // it will specify the subscription position anyway
        consumer.acknowledgeCumulativeAsync(msg);
        return msg;
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> msg = consumer.receive(timeout, unit);

        if (msg != null) {
            consumer.acknowledgeCumulativeAsync(msg);
        }
        return msg;
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return consumer.receiveAsync().thenApply(msg -> {
            consumer.acknowledgeCumulativeAsync(msg);
            return msg;
        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return consumer.closeAsync();
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return consumer.hasReachedEndOfTopic();
    }

    @Override
    public boolean hasMessageAvailable() throws PulsarClientException {
        return consumer.hasMessageAvailable() || consumer.numMessagesInQueue() > 0;
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return consumer.isConnected();
    }

    @Override
    public void seek(MessageId messageId) throws PulsarClientException {
        MessageIdImpl targetMessageId = MessageIdImpl.convertToMessageIdImpl(messageId);
        if (targetMessageId == null || isIllegalMultiTopicsReaderMessageId(messageId)) {
            throw new PulsarClientException("Illegal messageId, messageId can only be earliest、latest or determine partition");
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(1);
        consumer.getConsumers().forEach(consumerImpl -> {
            if (MessageId.latest.equals(targetMessageId) || MessageId.earliest.equals(targetMessageId)) {
                consumerImpl.resumeReceivingMessage();
                futures.add(consumerImpl.seekAsync(targetMessageId));
            } else if (consumerImpl.getPartitionIndex() == targetMessageId.getPartitionIndex()) {
                consumerImpl.resumeReceivingMessage();
                futures.add(consumerImpl.seekAsync(targetMessageId));
            } else {
                consumerImpl.pauseReceivingMessage();
            }
        });
        consumer.getUnAckedMessageTracker().clear();
        consumer.incomingMessages.clear();
        MultiTopicsConsumerImpl.INCOMING_MESSAGES_SIZE_UPDATER.set(consumer, 0);
        try {
            FutureUtil.waitForAll(futures).get();
        } catch (Exception e) {
            throw PulsarClientException.unwrap(e);
        }

    }

    @Override
    public void seek(long timestamp) throws PulsarClientException {
        consumer.seek(timestamp);
    }

    @Override
    public CompletableFuture<Void> seekAsync(MessageId messageId) {
        return consumer.seekAsync(messageId);
    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return consumer.seekAsync(timestamp);
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    public MultiTopicsConsumerImpl<T> getConsumer() {
        return consumer;
    }
}
