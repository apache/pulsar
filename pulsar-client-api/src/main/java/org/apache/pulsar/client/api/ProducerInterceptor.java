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
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A plugin interface that allows you to intercept (and possibly mutate) the
 * messages received by the producer before they are published to the Pulsar
 * brokers.
 *
 * <p>Exceptions thrown by ProducerInterceptor methods will be caught, logged, but
 * not propagated further.
 *
 * <p>ProducerInterceptor callbacks may be called from multiple threads. Interceptor
 * implementation must ensure thread-safety, if needed.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Deprecated
public interface ProducerInterceptor<T> extends AutoCloseable {

    /**
     * Close the interceptor.
     */
    void close();

    /**
     * This is called from {@link Producer#send(Object)} and {@link
     * Producer#sendAsync(Object)} methods, before
     * send the message to the brokers. This method is allowed to modify the
     * record, in which case, the new record
     * will be returned.
     *
     * <p>Any exception thrown by this method will be caught by the caller and
     * logged, but not propagated further.
     *
     * <p>Since the producer may run multiple interceptors, a particular
     * interceptor's {@link #beforeSend(Producer, Message)} callback will be called in the
     * order specified by
     * {@link ProducerBuilder#intercept(ProducerInterceptor[])}.
     *
     * <p>The first interceptor in the list gets the message passed from the client,
     * the following interceptor will be passed the message returned by the
     * previous interceptor, and so on. Since interceptors are allowed to modify
     * messages, interceptors may potentially get the message already modified by
     * other interceptors. However, building a pipeline of mutable interceptors
     * that depend on the output of the previous interceptor is discouraged,
     * because of potential side-effects caused by interceptors potentially
     * failing to modify the message and throwing an exception. If one of the
     * interceptors in the list throws an exception from
     * {@link#beforeSend(Message)}, the exception is caught, logged, and the next
     * interceptor is called with the message returned by the last successful
     * interceptor in the list, or otherwise the client.
     *
     * @param producer the producer which contains the interceptor.
     * @param message message to send
     * @return the intercepted message
     */
    Message<T> beforeSend(Producer<T> producer, Message<T> message);

    /**
     * This method is called when the message sent to the broker has been
     * acknowledged, or when sending the message fails.
     * This method is generally called just before the user callback is
     * called, and in additional cases when an exception on the producer side.
     *
     * <p>Any exception thrown by this method will be ignored by the caller.
     *
     * <p>This method will generally execute in the background I/O thread, so the
     * implementation should be reasonably fast. Otherwise, sending of messages
     * from other threads could be delayed.
     *
     * @param producer the producer which contains the interceptor.
     * @param message the message that application sends
     * @param msgId the message id that assigned by the broker; null if send failed.
     * @param exception the exception on sending messages, null indicates send has succeed.
     */
    void onSendAcknowledgement(Producer<T> producer, Message<T> message, MessageId msgId, Throwable exception);

    /**
     * This method is called when partitions of the topic (partitioned-topic) changes.
     *
     * @param topicName topic name
     * @param partitions new updated partitions
     */
    default void onPartitionsChange(String topicName, int partitions) {
    }

}
