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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A container that holds the list{@link ProducerInterceptor}
 * and wraps calls to the chain of custom interceptors.
 */
public class ProducerInterceptors implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProducerInterceptors.class);

    private final List<ProducerInterceptor> interceptors;

    public ProducerInterceptors(List<ProducerInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * This is called when client sends message to pulsar broker, before key and value gets serialized.
     * The method calls {@link ProducerInterceptor#beforeSend(Producer,Message)} method. Message returned from
     * first interceptor's beforeSend() is passed to the second interceptor beforeSend(), and so on in the
     * interceptor chain. The message returned from the last interceptor is returned from this method.
     *
     * This method does not throw exceptions. Exceptions thrown by any interceptor methods are caught and ignored.
     * If a interceptor in the middle of the chain, that normally modifies the message, throws an exception,
     * the next interceptor in the chain will be called with a message returned by the previous interceptor that did
     * not throw an exception.
     *
     * @param producer the producer which contains the interceptor.
     * @param message the message from client
     * @return the message to send to topic/partition
     */
    public Message beforeSend(Producer producer, Message message) {
        Message interceptorMessage = message;
        for (ProducerInterceptor interceptor : interceptors) {
            if (!interceptor.eligible(message)) {
                continue;
            }
            try {
                interceptorMessage = interceptor.beforeSend(producer, interceptorMessage);
            } catch (Throwable e) {
                if (producer != null) {
                    log.warn("Error executing interceptor beforeSend callback for topicName:{} ", producer.getTopic(), e);
                } else {
                    log.warn("Error Error executing interceptor beforeSend callback ", e);
                }
            }
        }
        return interceptorMessage;
    }

    /**
     * This method is called when the message send to the broker has been acknowledged, or when sending the record fails
     * before it gets send to the broker.
     * This method calls {@link ProducerInterceptor#onSendAcknowledgement(Producer, Message, MessageId, Throwable)} method for
     * each interceptor.
     *
     * This method does not throw exceptions. Exceptions thrown by any of interceptor methods are caught and ignored.
     *
     * @param producer the producer which contains the interceptor.
     * @param message The message returned from the last interceptor is returned from {@link ProducerInterceptor#beforeSend(Producer, Message)}
     * @param msgId The message id that broker returned. Null if has error occurred.
     * @param exception The exception thrown during processing of this message. Null if no error occurred.
     */
    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception) {
        for (ProducerInterceptor interceptor : interceptors) {
            if (!interceptor.eligible(message)) {
                continue;
            }
            try {
                interceptor.onSendAcknowledgement(producer, message, msgId, exception);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onSendAcknowledgement callback ", e);
            }
        }
    }

    public void onPartitionsChange(String topicName, int partitions) {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.onPartitionsChange(topicName, partitions);
            } catch (Throwable e) {
                log.warn("Error executing interceptor onPartitionsChange callback ", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (ProducerInterceptor interceptor : interceptors) {
            try {
                interceptor.close();
            } catch (Throwable e) {
                log.error("Fail to close producer interceptor ", e);
            }
        }
    }
}
