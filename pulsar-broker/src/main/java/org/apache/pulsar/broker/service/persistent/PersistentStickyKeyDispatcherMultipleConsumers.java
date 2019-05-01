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
package org.apache.pulsar.broker.service.persistent;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Consumer.SendMessageInfo;
import org.apache.pulsar.broker.service.HashRangeStickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PersistentStickyKeyDispatcherMultipleConsumers extends PersistentDispatcherMultipleConsumers {

    public static final String NONE_KEY = "NONE_KEY";

    private final StickyKeyConsumerSelector selector;

    PersistentStickyKeyDispatcherMultipleConsumers(PersistentTopic topic, ManagedCursor cursor) {
        super(topic, cursor);
        //TODO: Consumer selector Pluggable
        selector = new HashRangeStickyKeyConsumerSelector();
    }

    @Override
    public synchronized void addConsumer(Consumer consumer) throws BrokerServiceException {
        super.addConsumer(consumer);
        selector.addConsumer(consumer);
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        super.removeConsumer(consumer);
        selector.removeConsumer(consumer);
    }

    @Override
    protected void sendMessagesToConsumers(ReadType readType, List<Entry> entries) {
        long totalMessagesSent = 0;
        long totalBytesSent = 0;
        if (entries.size() > 0) {
            final Map<byte[], List<Entry>> groupedEntries = entries
                    .stream()
                    .collect(Collectors.groupingBy(entry -> peekStickyKey(entry.getDataBuffer()), Collectors.toList()));
            final Iterator<Map.Entry<byte[], List<Entry>>> iterator = groupedEntries.entrySet().iterator();
            while (iterator.hasNext() && totalAvailablePermits > 0 && isAtleastOneConsumerAvailable()) {
                final Map.Entry<byte[], List<Entry>> entriesWithSameKey = iterator.next();
                //TODO: None key policy
                final Consumer consumer = selector.select(entriesWithSameKey.getKey());

                if (consumer == null) {
                    // Do nothing, cursor will be rewind at reconnection
                    log.info("[{}] rewind because no available consumer found for key {} from total {}", name,
                            entriesWithSameKey.getKey(), consumerList.size());
                    entriesWithSameKey.getValue().forEach(Entry::release);
                    cursor.rewind();
                    return;
                }

                int messagesForC = Math.min(entriesWithSameKey.getValue().size(), consumer.getAvailablePermits());
                if (messagesForC > 0) {

                    // remove positions first from replay list first : sendMessages recycles entries
                    List<Entry> subList = new ArrayList<>(entriesWithSameKey.getValue().subList(0, messagesForC));
                    if (readType == ReadType.Replay) {
                        subList.forEach(entry -> messagesToRedeliver.remove(entry.getLedgerId(), entry.getEntryId()));
                    }
                    final SendMessageInfo sentMsgInfo = consumer.sendMessages(subList);
                    entriesWithSameKey.getValue().removeAll(subList);
                    final long msgSent = sentMsgInfo.getTotalSentMessages();
                    totalAvailablePermits -= msgSent;
                    totalMessagesSent += sentMsgInfo.getTotalSentMessages();
                    totalBytesSent += sentMsgInfo.getTotalSentMessageBytes();

                    if (entriesWithSameKey.getValue().size() == 0) {
                        iterator.remove();
                    }
                }
            }

            // acquire message-dispatch permits for already delivered messages
            if (serviceConfig.isDispatchThrottlingOnNonBacklogConsumerEnabled() || !cursor.isActive()) {
                if (topic.getDispatchRateLimiter().isPresent()) {
                    topic.getDispatchRateLimiter().get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
                }

                if (dispatchRateLimiter.isPresent()) {
                    dispatchRateLimiter.get().tryDispatchPermit(totalMessagesSent, totalBytesSent);
                }
            }

            if (groupedEntries.size() > 0) {
                int laterReplay = 0;
                for (List<Entry> entryList : groupedEntries.values()) {
                    laterReplay += entryList.size();
                    entryList.forEach(entry -> {
                        messagesToRedeliver.add(entry.getLedgerId(), entry.getEntryId());
                        entry.release();
                    });
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] No consumers found with available permits, storing {} positions for later replay", name,
                            laterReplay);
                }

            }
        }
    }

    @Override
    public SubType getType() {
        return SubType.Key_Shared;
    }

    private byte[] peekStickyKey(ByteBuf metadataAndPayload) {
        metadataAndPayload.markReaderIndex();
        MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);
        metadataAndPayload.resetReaderIndex();
        String key = metadata.getPartitionKey();
        metadata.recycle();
        if (StringUtils.isNotBlank(key) || metadata.hasOrderingKey()) {
            return metadata.hasOrderingKey() ? metadata.getOrderingKey().toByteArray() : key.getBytes();
        } else {
            return NONE_KEY.getBytes();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentStickyKeyDispatcherMultipleConsumers.class);

}
