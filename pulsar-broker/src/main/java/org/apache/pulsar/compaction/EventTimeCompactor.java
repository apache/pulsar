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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RawMessage;
import org.apache.pulsar.client.api.RawReader;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.RawBatchConverter;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventTimeCompactor extends TwoPhaseCompactor {

  private static final Logger log = LoggerFactory.getLogger(EventTimeCompactor.class);

  public EventTimeCompactor(ServiceConfiguration conf,
      PulsarClient pulsar,
      BookKeeper bk,
      ScheduledExecutorService scheduler) {
    super(conf, pulsar, bk, scheduler);
  }

  @Override
  protected CompletableFuture<TwoPhaseCompactor.PhaseOneResult> phaseOne(RawReader reader) {
    Map<String, Pair<MessageId, Long>> latestForKey = new HashMap<>();
    CompletableFuture<TwoPhaseCompactor.PhaseOneResult> loopPromise = new CompletableFuture<>();

    reader.getLastMessageIdAsync()
        .thenAccept(lastMessageId -> {
          log.info("Commencing phase one of compaction for {}, reading to {}",
              reader.getTopic(), lastMessageId);
          // Each entry is processed as a whole, discard the batchIndex part deliberately.
          MessageIdImpl lastImpl = (MessageIdImpl) lastMessageId;
          MessageIdImpl lastEntryMessageId = new MessageIdImpl(lastImpl.getLedgerId(),
              lastImpl.getEntryId(),
              lastImpl.getPartitionIndex());
          phaseOneLoop(reader, Optional.empty(), Optional.empty(), lastEntryMessageId, latestForKey,
              loopPromise);
        }).exceptionally(ex -> {
          loopPromise.completeExceptionally(ex);
          return null;
        });

    return loopPromise;
  }

  private void phaseOneLoop(RawReader reader,
      Optional<MessageId> firstMessageId,
      Optional<MessageId> toMessageId,
      MessageId lastMessageId,
      Map<String, Pair<MessageId, Long>> latestForKey,
      CompletableFuture<TwoPhaseCompactor.PhaseOneResult> loopPromise) {
    if (loopPromise.isDone()) {
      return;
    }
    CompletableFuture<RawMessage> future = reader.readNextAsync();
    FutureUtil.addTimeoutHandling(future,
        phaseOneLoopReadTimeout, scheduler,
        () -> FutureUtil.createTimeoutException("Timeout", getClass(), "phaseOneLoop(...)"));

    future.thenAcceptAsync(m -> {
      try (m) {
        MessageId id = m.getMessageId();
        boolean deletedMessage = false;
        boolean replaceMessage = false;
        mxBean.addCompactionReadOp(reader.getTopic(), m.getHeadersAndPayload().readableBytes());
        MessageMetadata metadata = Commands.parseMessageMetadata(m.getHeadersAndPayload());
        if (Markers.isServerOnlyMarker(metadata)) {
          mxBean.addCompactionRemovedEvent(reader.getTopic());
          deletedMessage = true;
        } else if (RawBatchConverter.isReadableBatch(metadata)) {
          try {
            int numMessagesInBatch = metadata.getNumMessagesInBatch();
            int deleteCnt = 0;

            for (MessageCompactionData mcd : extractMessageCompactionDataFromBatch(m)) {
              if (mcd.key() == null) {
                if (!topicCompactionRetainNullKey) {
                  // record delete null-key message event
                  deleteCnt++;
                  mxBean.addCompactionRemovedEvent(reader.getTopic());
                }
                continue;
              }

              boolean newer = Optional.ofNullable(latestForKey.get(mcd.key()))
                  .map(Pair::getRight)
                      .map(latestEventTime -> mcd.eventTime() != null
                      && mcd.eventTime() > latestEventTime).orElse(true);
              if (newer) {
                if (mcd.payloadSize() > 0) { //message from batch has payload (is not null)
                  Pair<MessageId, Long> old = latestForKey.put(mcd.key(),
                      new ImmutablePair<>(mcd.messageId(), mcd.eventTime()));
                  if (old != null) {
                    mxBean.addCompactionRemovedEvent(reader.getTopic());
                  }
                } else { //handling null message from batch
                  latestForKey.remove(mcd.key());
                  deleteCnt++;
                  mxBean.addCompactionRemovedEvent(reader.getTopic());
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
        } else {
          MessageCompactionData mcd = extractMessageCompactionData(m);

          if (mcd != null) {
            boolean newer = Optional.ofNullable(latestForKey.get(mcd.key()))
                .map(Pair::getRight)
                .map(latestEventTime -> mcd.eventTime() != null
                    && mcd.eventTime() > latestEventTime).orElse(true);
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
            mxBean.addCompactionRemovedEvent(reader.getTopic());
          }
        }
        MessageId first = firstMessageId.orElse(deletedMessage ? null : id);
        MessageId to = deletedMessage ? toMessageId.orElse(null) : id;
        if (id.compareTo(lastMessageId) == 0) {
          Map<String, MessageId> latestIdForKey = latestForKey.entrySet().stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey,
                  entry -> entry.getValue().getLeft()
              ));
          loopPromise.complete(
              new TwoPhaseCompactor.PhaseOneResult(first == null ? id : first, to == null ? id : to,
                  lastMessageId, latestIdForKey));
        } else {
          phaseOneLoop(reader,
              Optional.ofNullable(first),
              Optional.ofNullable(to),
              lastMessageId,
              latestForKey, loopPromise);
        }
      }
    }, scheduler).exceptionally(ex -> {
      loopPromise.completeExceptionally(ex);
      return null;
    });
  }

  protected MessageCompactionData extractMessageCompactionData(RawMessage m) {
    ByteBuf headersAndPayload = m.getHeadersAndPayload();
    MessageMetadata msgMetadata = Commands.parseMessageMetadata(headersAndPayload);
    if (msgMetadata.hasPartitionKey()) {
      int size = headersAndPayload.readableBytes();
      if (msgMetadata.hasUncompressedSize()) {
        size = msgMetadata.getUncompressedSize();
      }
      return new MessageCompactionData(m.getMessageId(), msgMetadata.getPartitionKey(),
          size, msgMetadata.getEventTime());
    } else {
      return null;
    }
  }

  protected List<MessageCompactionData> extractMessageCompactionDataFromBatch(RawMessage msg)
      throws IOException {
    return RawBatchConverter.extractMessageCompactionData(msg);
  }
}