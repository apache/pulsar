/*
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
 * ReaderInterceptorUtil is used to wrap ReaderInterceptor by ConsumerInterceptor.
 */
public class ReaderInterceptorUtil {

    public static <T> ConsumerInterceptors<T> convertToConsumerInterceptors(
            Reader<T> reader, List<ReaderInterceptor<T>> interceptorList) {
        if (interceptorList == null || interceptorList.isEmpty()) {
            return null;
        }
        List<ConsumerInterceptor<T>> consumerInterceptorList = new ArrayList<>(interceptorList.size());
        for (ReaderInterceptor<T> readerInterceptor : interceptorList) {
            consumerInterceptorList.add(getInterceptor(reader, readerInterceptor));
        }
        return new ConsumerInterceptors<>(consumerInterceptorList);
    }

    private static <T> ConsumerInterceptor<T> getInterceptor(Reader<T> reader, ReaderInterceptor<T> readerInterceptor) {
        return new ConsumerInterceptor<T>() {
            @Override
            public void close() {
                readerInterceptor.close();
            }

            @Override
            public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
                return readerInterceptor.beforeRead(reader, message);
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
                readerInterceptor.onPartitionsChange(topicName, partitions);
            }
        };
    }

}
