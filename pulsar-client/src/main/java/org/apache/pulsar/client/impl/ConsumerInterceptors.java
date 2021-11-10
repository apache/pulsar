/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * A container that hold the list {@link org.apache.pulsar.client.api.ConsumerInterceptor} and wraps calls to the chain
 * of custom interceptors.
 */
public class ConsumerInterceptors<T> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerInterceptors.class);

    private final List<ConsumerInterceptor<T>> interceptors;

    public ConsumerInterceptors(List<ConsumerInterceptor<T>> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called just before the message is returned by {@link Consumer#receive()},
     * {@link MessageListener#received(Consumer, Message)} or the {@link java.util.concurrent.CompletableFuture}
     * returned by {@link Consumer#receiveAsync()} completes.
     * <p>
     * This method calls {@link ConsumerInterceptor#beforeConsume(Consumer, Message)} for each interceptor. Messages returned
     * from each interceptor get passed to beforeConsume() of the next interceptor in the chain of interceptors.
     * <p>
     * This method does not throw exceptions. If any of the interceptors in the chain throws an exception, it gets
     * caught and logged, and next interceptor in int the chain is called with 'messages' returned by the previous
     * successful interceptor beforeConsume call.
     *
     * @param consumer the consumer which contains the interceptors
     * @param message message to be consume by the client.
     * @return messages that are either modified by interceptors or same as messages passed to this method.
     */
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        Message<T> interceptorMessage = message;
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptorMessage = interceptors.get(i).beforeConsume(consumer, interceptorMessage);
            } catch (Throwable e) {
                if (consumer != null) {
                    log.warn("Error executing interceptor beforeConsume callback topic: {} consumerName: {}", consumer.getTopic(), consumer.getConsumerName(), e);
                } else {
                    log.warn("Error executing interceptor beforeConsume callback", e);
                }
            }
        }
        return interceptorMessage;
    }

    /**
     * This is called when acknowledge request return from the broker.
     * <p>
     * This method calls {@link ConsumerInterceptor#onAcknowledge(Consumer, MessageId, Throwable)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param consumer the consumer which contains the interceptors
     * @param messageId message to acknowledge.
     * @param exception exception returned by broker.
     */
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).onAcknowledge(consumer, messageId, exception);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onAcknowledge callback ", e);
            }
        }
    }

    /**
     * This is called when acknowledge cumulative request return from the broker.
     * <p>
     * This method calls {@link ConsumerInterceptor#onAcknowledgeCumulative(Consumer, MessageId, Throwable)} (Message, Throwable)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param consumer the consumer which contains the interceptors
     * @param messageId messages to acknowledge.
     * @param exception exception returned by broker.
     */
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).onAcknowledgeCumulative(consumer, messageId, exception);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onAcknowledgeCumulative callback ", e);
            }
        }
    }

    /**
     * This is called when a redelivery from a negative acknowledge occurs.
     * <p>
     * This method calls {@link ConsumerInterceptor#onNegativeAcksSend(Consumer, Set)
     * onNegativeAcksSend(Consumer, Set&lt;MessageId&gt;)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param consumer the consumer which contains the interceptors.
     * @param messageIds set of message IDs being redelivery due a negative acknowledge.
     */
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).onNegativeAcksSend(consumer, messageIds);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onNegativeAcksSend callback", e);
            }
        }
    }

    /**
     * This is called when a redelivery from an acknowledge timeout occurs.
     * <p>
     * This method calls {@link ConsumerInterceptor#onAckTimeoutSend(Consumer, Set)
     * onAckTimeoutSend(Consumer, Set&lt;MessageId&gt;)} method for each interceptor.
     * <p>
     * This method does not throw exceptions. Exceptions thrown by any of interceptors in the chain are logged, but not propagated.
     *
     * @param consumer the consumer which contains the interceptors.
     * @param messageIds set of message IDs being redelivery due an acknowledge timeout.
     */
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).onAckTimeoutSend(consumer, messageIds);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onAckTimeoutSend callback", e);
            }
        }
    }

    public void onPartitionsChange(String topicName, int partitions) {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).onPartitionsChange(topicName, partitions);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onPartitionsChange callback", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (int i = 0, interceptorsSize = interceptors.size(); i < interceptorsSize; i++) {
            try {
                interceptors.get(i).close();
            } catch (Throwable e) {
                log.error("Fail to close consumer interceptor ", e);
            }
        }
    }

}
