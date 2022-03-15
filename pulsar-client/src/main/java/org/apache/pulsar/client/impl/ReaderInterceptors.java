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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderInterceptor;


/**
 * Reader interceptors.
 * @param <T>
 */
public class ReaderInterceptors<T> implements Closeable {

    private final Reader<T> reader;
    private final List<ReaderInterceptor<T>> interceptorList;

    public ReaderInterceptors(Reader<T> reader, List<ReaderInterceptor<T>> interceptorList) {
        this.reader = reader;
        if (interceptorList == null) {
            this.interceptorList = new ArrayList<>();
        } else {
            this.interceptorList = interceptorList;
        }
    }

    @Override
    public void close() {
        for (ReaderInterceptor<T> interceptor : interceptorList) {
            interceptor.close();
        }
    }

    public ConsumerInterceptors<T> convertToConsumerInterceptor() {
        List<ConsumerInterceptor<T>> consumerInterceptorList = new ArrayList<>();
        for (ReaderInterceptor<T> interceptor : interceptorList) {
            consumerInterceptorList.add(
                    new ConsumerInterceptor<T>() {
                        @Override
                        public void close() {
                            interceptor.close();
                        }

                        @Override
                        public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
                            return interceptor.beforeRead(reader, message);
                        }

                        @Override
                        public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
                            // nothing to do
                        }

                        @Override
                        public void onAcknowledgeCumulative(Consumer<T> consumer,
                                                            MessageId messageId, Throwable exception) {
                            // nothing to do
                        }

                        @Override
                        public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> set) {
                            // nothing to do
                        }

                        @Override
                        public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> set) {
                            // nothing to do
                        }

                        @Override
                        public void onPartitionsChange(String topicName, int partitions) {
                            interceptor.onPartitionsChange(topicName, partitions);
                        }
                    }
            );
        }
        return new ConsumerInterceptors<T>(consumerInterceptorList);
    }

}
