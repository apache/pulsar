package org.apache.pulsar.grpc;

import com.google.common.base.Enums;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.grpc.proto.ConsumerAck;
import org.apache.pulsar.grpc.proto.ConsumerMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.grpc.Constant.GRPC_PROXY_CTX_KEY;

public class ConsumerHandler implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerHandler.class);

    private final GrpcService service;
    private final StreamObserver<ConsumerMessage> messageStreamObserver;
    private String topic;
    private String subscription;
    private Consumer<byte[]> consumer;

    private int maxPendingMessages;
    private final AtomicInteger pendingMessages = new AtomicInteger();

    private volatile long msgDeliveredCounter = 0;
    private static final AtomicLongFieldUpdater<ConsumerHandler> MSG_DELIVERED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(ConsumerHandler.class, "msgDeliveredCounter");

    ConsumerHandler(GrpcService service, StreamObserver<ConsumerMessage> messageStreamObserver) {
        this.service = service;
        this.messageStreamObserver = messageStreamObserver;
        Map<String, String> params = GRPC_PROXY_CTX_KEY.get();

        try {
            this.topic = params.get("pulsar-topic");
            checkArgument(!Strings.isNullOrEmpty(topic), "Empty topic name");
            ConsumerBuilderImpl<byte[]> builder = (ConsumerBuilderImpl<byte[]>) getConsumerConfiguration(params, service.getPulsarClient());
            this.maxPendingMessages = (builder.getConf().getReceiverQueueSize() == 0) ? 1
                    : builder.getConf().getReceiverQueueSize();

            // checkAuth() should be called after assigning a value to this.subscription
            this.subscription = params.get("pulsar-subscription");
            checkArgument(!Strings.isNullOrEmpty(subscription), "Empty subscription name");

            // TODO: authN/authZ
            /*if (!checkAuth(response)) {
                return;
            }*/

            builder.topic(params.get("pulsar-topic"))
                    .subscriptionName(subscription);
            consumer = builder.subscribe();
        } catch (Exception e) {
            throw getStatus(e).withDescription(getErrorMessage(e)).asRuntimeException();
        }
    }

    public StreamObserver<ConsumerAck> consume() {
        receiveMessage();
        return new StreamObserver<ConsumerAck>() {
            @Override
            public void onNext(ConsumerAck ack) {
                MessageId msgId;
                try {
                    msgId = MessageId.fromByteArrayWithTopic(
                            ack.getMessageId().toByteArray(),
                            TopicName.get(consumer.getTopic()));
                } catch (IOException e) {
                    log.warn("Failed to deserialize message id: {}", ack.getMessageId(), e);
                    messageStreamObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
                    throw Status.INVALID_ARGUMENT.asRuntimeException();
                }
                consumer.acknowledgeAsync(msgId).thenAccept(consumer -> {
                    log.error("acknowledged " + msgId);
                    //numMsgsAcked.increment();
                });

                int pending = pendingMessages.getAndDecrement();
                if (pending >= maxPendingMessages) {
                    // Resume delivery
                    receiveMessage();
                }
            }

            @Override
            public void onError(Throwable t) {
                messageStreamObserver.onError(t);
                close();
            }

            @Override
            public void onCompleted() {
                messageStreamObserver.onCompleted();
                close();
            }
        };
    }

    private static Status getStatus(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return  Status.INVALID_ARGUMENT;
        } else if (e instanceof PulsarClientException.ConsumerBusyException) {
            return  Status.ALREADY_EXISTS;
        } else {
            return Status.INTERNAL;
        }
    }

    private static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException) {
            return "Invalid header params: " + e.getMessage();
        } else {
            return "Failed to subscribe: " + e.getMessage();
        }
    }

    private void receiveMessage() {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Receive next message",
                    //request.getRemoteAddr(),
                    //request.getRemotePort(),
                    consumer.getTopic(),
                    consumer.getSubscription());
        }

        consumer.receiveAsync().thenAccept(msg -> {
            if (log.isDebugEnabled()) {
                // TODO: get remote address
                log.debug("[{}] [{}] Got message {}"/*, getSession().getRemoteAddress()*/,
                        consumer.getTopic(),
                        consumer.getSubscription(),
                        msg.getMessageId());
            }

            ConsumerMessage.Builder dm = ConsumerMessage.newBuilder();
            dm.setMessageId(ByteString.copyFrom(msg.getMessageId().toByteArray()));
            dm.setPayload(ByteString.copyFrom(msg.getData()));
            dm.putAllProperties(msg.getProperties());
            dm.setPublishTime(msg.getPublishTime());
            dm.setEventTime(msg.getEventTime());
            //TODO: needs proto hasKey or empty string is OK
            if (msg.hasKey()) {
                dm.setKey(msg.getKey());
            }
            //final long msgSize = msg.getData().length;

            //TODO: thread safety ?
            messageStreamObserver.onNext(dm.build());

            int pending = pendingMessages.incrementAndGet();
            if (pending < maxPendingMessages) {
                // Start next read in a separate thread to avoid recursion
                // TODO: needed ?
                service.getExecutor().execute(this::receiveMessage);
            }
        }).exceptionally(exception -> {
            if (exception.getCause() instanceof PulsarClientException.AlreadyClosedException) {
                log.info("[{}/{}] Consumer was closed while receiving msg from broker", consumer.getTopic(),
                        subscription);
            } else {
                log.warn("[{}/{}] Error occurred while consumer handler was delivering msg: {}",
                        consumer.getTopic(), subscription,
                        exception.getMessage());
            }
            return null;
        });
    }

    private ConsumerBuilder<byte[]> getConsumerConfiguration(Map<String, String> headerParams, PulsarClient client) {
        ConsumerBuilder<byte[]> builder = client.newConsumer();

        if (headerParams.containsKey("pulsar-ack-timeout-millis")) {
            builder.ackTimeout(Integer.parseInt(headerParams.get("pulsar-ack-timeout-millis")), TimeUnit.MILLISECONDS);
        }

        if (headerParams.containsKey("pulsar-subscription-type")) {
            checkArgument(Enums.getIfPresent(SubscriptionType.class, headerParams.get("pulsar-subscription-type")).isPresent(),
                    "Invalid subscriptionType %s", headerParams.get("pulsar-subscription-type"));
            builder.subscriptionType(SubscriptionType.valueOf(headerParams.get("pulsar-subscription-type")));
        }

        if (headerParams.containsKey("pulsar-receiver-queue-size")) {
            builder.receiverQueueSize(Math.min(Integer.parseInt(headerParams.get("pulsar-receiver-queue-size")), 1000));
        }

        if (headerParams.containsKey("pulsar-consumer-name")) {
            builder.consumerName(headerParams.get("pulsar-consumer-name"));
        }

        if (headerParams.containsKey("pulsar-priority-level")) {
            builder.priorityLevel(Integer.parseInt(headerParams.get("pulsar-priority-level")));
        }

        if (headerParams.containsKey("pulsar-max-redeliver-count") || headerParams.containsKey("pulsar-dead-letter-topic")) {
            DeadLetterPolicy.DeadLetterPolicyBuilder dlpBuilder = DeadLetterPolicy.builder();
            if (headerParams.containsKey("pulsar-max-redeliver-count")) {
                dlpBuilder.maxRedeliverCount(Integer.parseInt(headerParams.get("pulsar-max-redeliver-count")))
                        .deadLetterTopic(String.format("%s-%s-DLQ", topic, subscription));
            }

            if (headerParams.containsKey("pulsar-dead-letter-topic")) {
                dlpBuilder.deadLetterTopic(headerParams.get("pulsar-dead-letter-topic"));
            }
            builder.deadLetterPolicy(dlpBuilder.build());
        }

        return builder;
    }

    @Override
    public void close() {
        if (consumer != null) {
            /*if (!this.service.removeConsumer(this)) {
                log.warn("[{}] Failed to remove consumer handler", consumer.getTopic());
            }*/
            consumer.closeAsync().thenAccept(x -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Closed consumer asynchronously", consumer.getTopic());
                }
            }).exceptionally(exception -> {
                log.warn("[{}] Failed to close consumer", consumer.getTopic(), exception);
                return null;
            });
        }
    }

}
