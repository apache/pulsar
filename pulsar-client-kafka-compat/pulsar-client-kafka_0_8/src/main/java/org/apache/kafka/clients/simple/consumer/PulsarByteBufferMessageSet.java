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
package org.apache.kafka.clients.simple.consumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import com.google.common.collect.Queues;

import kafka.message.MessageAndOffset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarByteBufferMessageSet extends kafka.javaapi.message.ByteBufferMessageSet {
    private final MessageAndOffsetIterator iterator;

    public PulsarByteBufferMessageSet(Reader<byte[]> reader) {
        super(Collections.emptyList());
        this.iterator = new MessageAndOffsetIterator(reader);
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return iterator;
    }

    public static class MessageAndOffsetIterator implements Iterator<MessageAndOffset> {

        private final Reader<byte[]> reader;
        private final ConcurrentLinkedQueue<org.apache.pulsar.client.api.Message<byte[]>> receivedMessages;

        public MessageAndOffsetIterator(Reader<byte[]> reader) {
            this.reader = reader;
            this.receivedMessages = Queues.newConcurrentLinkedQueue();
        }

        @Override
        public boolean hasNext() {
            try {
                org.apache.pulsar.client.api.Message<byte[]> msg = reader.readNext(10, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    receivedMessages.offer(msg);
                    return true;
                }
            } catch (PulsarClientException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to receive message for {}, {}", reader.getTopic(), e.getMessage());
                }
            }
            return false;
        }

        @Override
        public PulsarMsgAndOffset next() {

            org.apache.pulsar.client.api.Message<byte[]> msg = receivedMessages.poll();
            if (msg == null) {
                try {
                    msg = reader.readNext();
                } catch (PulsarClientException e) {
                    log.warn("Failed to receive message for {}, {}", reader.getTopic(), e.getMessage(), e);
                    throw new RuntimeException("failed to receive message from " + reader.getTopic());
                }
            }

            String key = msg.getKey();
            byte[] value = msg.getValue();

            return new PulsarMsgAndOffset(new PulsarMessage(key, value), msg.getMessageId());
        }
    }
}