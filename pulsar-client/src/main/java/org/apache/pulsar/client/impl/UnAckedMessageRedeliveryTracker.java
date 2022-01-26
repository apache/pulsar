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

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAckedMessageRedeliveryTracker extends UnAckedMessageTracker {

    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageRedeliveryTracker.class);

    protected final HashMap<UnackMessageIdWrapper, HashSet<UnackMessageIdWrapper>> redeliveryMessageIdPartitionMap;
    protected final ArrayDeque<HashSet<UnackMessageIdWrapper>> redeliveryTimePartitions;

    protected final HashMap<MessageId, Long> ackTimeoutMessages;
    private final RedeliveryBackoff ackTimeoutRedeliveryBackoff;

    public UnAckedMessageRedeliveryTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase,
                                           ConsumerConfigurationData<?> conf) {
        super(client, consumerBase, conf);
        this.ackTimeoutRedeliveryBackoff = conf.getAckTimeoutRedeliveryBackoff();
        this.ackTimeoutMessages = new HashMap<MessageId, Long>();
        this.redeliveryMessageIdPartitionMap = new HashMap<>();
        this.redeliveryTimePartitions = new ArrayDeque<>();

        int blankPartitions = (int) Math.ceil((double) this.ackTimeoutMillis / this.tickDurationInMs);
        for (int i = 0; i < blankPartitions + 1; i++) {
            redeliveryTimePartitions.add(new HashSet<>(16, 1));
        }

        timeout = client.timer().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                writeLock.lock();
                try {
                    HashSet<UnackMessageIdWrapper> headPartition = redeliveryTimePartitions.removeFirst();
                    if (!headPartition.isEmpty()) {
                        headPartition.forEach(unackMessageIdWrapper -> {
                            addAckTimeoutMessages(unackMessageIdWrapper);
                            redeliveryMessageIdPartitionMap.remove(unackMessageIdWrapper);
                            unackMessageIdWrapper.recycle();
                        });
                    }
                    headPartition.clear();
                    redeliveryTimePartitions.addLast(headPartition);
                    triggerRedelivery(consumerBase);
                } finally {
                    writeLock.unlock();
                    timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
                }
            }
        }, this.tickDurationInMs, TimeUnit.MILLISECONDS);

    }

    private void addAckTimeoutMessages(UnackMessageIdWrapper messageIdWrapper) {
        writeLock.lock();
        try {
            MessageId messageId = messageIdWrapper.getMessageId();
            int redeliveryCount = messageIdWrapper.getRedeliveryCount();
            long backoffNs = ackTimeoutRedeliveryBackoff.next(redeliveryCount);
            ackTimeoutMessages.put(messageId, System.currentTimeMillis() + backoffNs);
        } finally {
            writeLock.unlock();
        }
    }

    private void triggerRedelivery(ConsumerBase<?> consumerBase) {
        if (ackTimeoutMessages.isEmpty()) {
            return;
        }
        Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
        messageIds.clear();

        try {
            long now = System.currentTimeMillis();
            ackTimeoutMessages.forEach((messageId, timestamp) -> {
                if (timestamp <= now) {
                    addChunkedMessageIdsAndRemoveFromSequenceMap(messageId, messageIds, consumerBase);
                    messageIds.add(messageId);
                }
            });
            if (!messageIds.isEmpty()) {
                log.info("[{}] {} messages will be re-delivered", consumerBase, messageIds.size());
                Iterator<MessageId> iterator = messageIds.iterator();
                while (iterator.hasNext()) {
                    MessageId messageId = iterator.next();
                    ackTimeoutMessages.remove(messageId);
                }
            }
        } finally {
            if (messageIds.size() > 0) {
                consumerBase.onAckTimeoutSend(messageIds);
                consumerBase.redeliverUnacknowledgedMessages(messageIds);
            }
        }
    }

    @Override
    boolean isEmpty() {
        readLock.lock();
        try {
            return redeliveryMessageIdPartitionMap.isEmpty() && ackTimeoutMessages.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            redeliveryMessageIdPartitionMap.clear();
            redeliveryTimePartitions.forEach(tp -> {
                        tp.forEach(unackMessageIdWrapper -> unackMessageIdWrapper.recycle());
                        tp.clear();
                    }
            );
            ackTimeoutMessages.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean add(MessageId messageId) {
        return add(messageId, 0);
    }

    @Override
    public boolean add(MessageId messageId, int redeliveryCount) {
        writeLock.lock();
        try {
            UnackMessageIdWrapper messageIdWrapper = UnackMessageIdWrapper.valueOf(messageId, redeliveryCount);
            HashSet<UnackMessageIdWrapper> partition = redeliveryTimePartitions.peekLast();
            HashSet<UnackMessageIdWrapper> previousPartition = redeliveryMessageIdPartitionMap
                    .putIfAbsent(messageIdWrapper, partition);
            if (previousPartition == null) {
                return partition.add(messageIdWrapper);
            } else {
                messageIdWrapper.recycle();
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean remove(MessageId messageId) {
        writeLock.lock();
        UnackMessageIdWrapper messageIdWrapper = UnackMessageIdWrapper.valueOf(messageId);
        try {
            boolean removed = false;
            HashSet<UnackMessageIdWrapper> exist =
                    redeliveryMessageIdPartitionMap.remove(messageIdWrapper);
            if (exist != null) {
                removed = exist.remove(messageIdWrapper);
            }
            return removed || ackTimeoutMessages.remove(messageId) != null;
        } finally {
            messageIdWrapper.recycle();
            writeLock.unlock();
        }
    }

    @Override
    long size() {
        readLock.lock();
        try {
            return redeliveryMessageIdPartitionMap.size() + ackTimeoutMessages.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<Entry<UnackMessageIdWrapper, HashSet<UnackMessageIdWrapper>>> iterator =
                    redeliveryMessageIdPartitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<UnackMessageIdWrapper, HashSet<UnackMessageIdWrapper>> entry = iterator.next();
                UnackMessageIdWrapper messageIdWrapper = entry.getKey();
                if (messageIdWrapper.getMessageId().compareTo(msgId) <= 0) {
                    entry.getValue().remove(messageIdWrapper);
                    iterator.remove();
                    messageIdWrapper.recycle();
                    removed++;
                }
            }

            Iterator<MessageId> iteratorAckTimeOut = ackTimeoutMessages.keySet().iterator();
            while (iteratorAckTimeOut.hasNext()) {
                MessageId messageId = iteratorAckTimeOut.next();
                if (messageId.compareTo(msgId) <= 0) {
                    iteratorAckTimeOut.remove();
                    removed++;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

}
