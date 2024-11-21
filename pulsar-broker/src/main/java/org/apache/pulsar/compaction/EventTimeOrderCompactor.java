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
package org.apache.pulsar.compaction;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.impl.RawBatchConverter;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTimeOrderCompactor extends AbstractTwoPhaseCompactor<Pair<MessageId, Long>> {

  private static final Logger log = LoggerFactory.getLogger(EventTimeOrderCompactor.class);

  public EventTimeOrderCompactor(ServiceConfiguration conf,
      PulsarClient pulsar,
      BookKeeper bk,
      ScheduledExecutorService scheduler) {
    super(conf, pulsar, bk, scheduler);
  }

  @Override
  protected Map<String, MessageId> toLatestMessageIdForKey(
      Map<String, Pair<MessageId, Long>> latestForKey) {
    return latestForKey.entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().getLeft()));
  }

  @Override
  protected boolean compactMessage(String topic, Map<String, Pair<MessageId, Long>> latestForKey,
      RawMessage m, MessageMetadata metadata, MessageId id) {
    boolean deletedMessage = false;
    boolean replaceMessage = false;
    MessageCompactionData mcd = extractMessageCompactionData(m, metadata);

    if (mcd != null) {
      boolean newer = Optional.ofNullable(latestForKey.get(mcd.key()))
          .map(Pair::getRight)
          .map(latestEventTime -> mcd.eventTime() != null
              && mcd.eventTime() >= latestEventTime).orElse(true);
      if (newer) {
        if (mcd.payloadSize() > 0) {
          Pair<MessageId, Long> old = latestForKey.put(mcd.key(),
              new ImmutablePair<>(mcd.messageId(), mcd.eventTime()));
          replaceMessage = old != null;
        } else {
          deletedMessage = true;
          latestForKey.remove(mcd.key());
        }
      }
    } else {
      if (!topicCompactionRetainNullKey) {
        deletedMessage = true;
      }
    }
    if (replaceMessage || deletedMessage) {
      mxBean.addCompactionRemovedEvent(topic);
    }
    return deletedMessage;
  }

  @Override
  protected boolean compactBatchMessage(String topic, Map<String, Pair<MessageId, Long>> latestForKey, RawMessage m,
      MessageMetadata metadata, MessageId id) {
    boolean deletedMessage = false;
    try {
      int numMessagesInBatch = metadata.getNumMessagesInBatch();
      int deleteCnt = 0;

      for (MessageCompactionData mcd : extractMessageCompactionDataFromBatch(m, metadata)) {
        if (mcd.key() == null) {
          if (!topicCompactionRetainNullKey) {
            // record delete null-key message event
            deleteCnt++;
            mxBean.addCompactionRemovedEvent(topic);
          }
          continue;
        }

        boolean newer = Optional.ofNullable(latestForKey.get(mcd.key()))
            .map(Pair::getRight)
            .map(latestEventTime -> mcd.eventTime() != null
                && mcd.eventTime() > latestEventTime).orElse(true);
        if (newer) {
          if (mcd.payloadSize() > 0) {
            Pair<MessageId, Long> old = latestForKey.put(mcd.key(),
                new ImmutablePair<>(mcd.messageId(), mcd.eventTime()));
            if (old != null) {
              mxBean.addCompactionRemovedEvent(topic);
            }
          } else {
            latestForKey.remove(mcd.key());
            deleteCnt++;
            mxBean.addCompactionRemovedEvent(topic);
          }
        }
      }

      if (deleteCnt == numMessagesInBatch) {
        deletedMessage = true;
      }
    } catch (IOException ioe) {
      log.info("Error decoding batch for message {}. Whole batch will be included in output",
          id, ioe);
    }
    return deletedMessage;
  }

  protected MessageCompactionData extractMessageCompactionData(RawMessage m, MessageMetadata metadata) {
    ByteBuf headersAndPayload = m.getHeadersAndPayload();
    if (metadata.hasPartitionKey()) {
      int size = headersAndPayload.readableBytes();
      if (metadata.hasUncompressedSize()) {
        size = metadata.getUncompressedSize();
      }
      return new MessageCompactionData(m.getMessageId(), metadata.getPartitionKey(),
          size, metadata.getEventTime());
    } else {
      return null;
    }
  }

  private List<MessageCompactionData> extractMessageCompactionDataFromBatch(RawMessage msg, MessageMetadata metadata)
      throws IOException {
    return RawBatchConverter.extractMessageCompactionData(msg, metadata);
  }
}