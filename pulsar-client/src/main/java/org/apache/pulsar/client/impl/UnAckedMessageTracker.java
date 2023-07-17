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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);

    protected final HashMap<MessageId, HashSet<MessageId>> messageIdPartitionMap;
    protected final ArrayDeque<HashSet<MessageId>> timePartitions;

    protected final Lock readLock;
    protected final Lock writeLock;

    public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED =
            new UnAckedMessageTrackerDisabled();
    protected final long ackTimeoutMillis;
    protected final long tickDurationInMs;

    private static class UnAckedMessageTrackerDisabled extends UnAckedMessageTracker {
        @Override
        public void clear() {
        }

        @Override
        long size() {
            return 0;
        }

        @Override
        public boolean add(MessageId m) {
            return true;
        }

        @Override
        public boolean add(MessageId messageId, int redeliveryCount) {
            return true;
        }

        @Override
        public boolean remove(MessageId m) {
            return true;
        }

        @Override
        public int removeMessagesTill(MessageId msgId) {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    protected Timeout timeout;

    public UnAckedMessageTracker() {
        readLock = null;
        writeLock = null;
        timePartitions = null;
        messageIdPartitionMap = null;
        this.ackTimeoutMillis = 0;
        this.tickDurationInMs = 0;
    }

    protected static final FastThreadLocal<HashSet<MessageId>> TL_MESSAGE_IDS_SET =
            new FastThreadLocal<HashSet<MessageId>>() {
        @Override
        protected HashSet<MessageId> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase,
                                 ConsumerConfigurationData<?> conf) {
        this.ackTimeoutMillis = conf.getAckTimeoutMillis();
        this.tickDurationInMs = Math.min(conf.getTickDurationMillis(), conf.getAckTimeoutMillis());
        checkArgument(tickDurationInMs > 0 && ackTimeoutMillis >= tickDurationInMs);
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        if (conf.getAckTimeoutRedeliveryBackoff() == null) {
            this.messageIdPartitionMap = new HashMap<>();
            this.timePartitions = new ArrayDeque<>();

            int blankPartitions = (int) Math.ceil((double) this.ackTimeoutMillis / this.tickDurationInMs);
            for (int i = 0; i < blankPartitions + 1; i++) {
                timePartitions.add(new HashSet<>(16, 1));
            }
            timeout = client.timer().newTimeout(new TimerTask() {
                @Override
                public void run(Timeout t) throws Exception {
                    if (t.isCancelled()) {
                        return;
                    }

                    Set<MessageId> messageIds = TL_MESSAGE_IDS_SET.get();
                    messageIds.clear();

                    writeLock.lock();
                    try {
                        HashSet<MessageId> headPartition = timePartitions.removeFirst();
                        if (!headPartition.isEmpty()) {
                            log.info("[{}] {} messages will be re-delivered", consumerBase, headPartition.size());
                            headPartition.forEach(messageId -> {
                                addChunkedMessageIdsAndRemoveFromSequenceMap(messageId, messageIds, consumerBase);
                                messageIds.add(messageId);
                                messageIdPartitionMap.remove(messageId);
                            });
                        }

                        headPartition.clear();
                        timePartitions.addLast(headPartition);
                    } finally {
                        try {
                            timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
                        } finally {
                            writeLock.unlock();

                            if (!messageIds.isEmpty()) {
                                consumerBase.onAckTimeoutSend(messageIds);
                                consumerBase.redeliverUnacknowledgedMessages(messageIds);
                            }
                        }
                    }
                }
            }, this.tickDurationInMs, TimeUnit.MILLISECONDS);
        } else {
            this.messageIdPartitionMap = null;
            this.timePartitions = null;
        }
    }

    public static void addChunkedMessageIdsAndRemoveFromSequenceMap(MessageId messageId, Set<MessageId> messageIds,
                                                                    ConsumerBase<?> consumerBase) {
        if (messageId instanceof MessageIdImpl) {
            MessageIdImpl[] chunkedMsgIds = consumerBase.unAckedChunkedMessageIdSequenceMap
                    .get((MessageIdImpl) messageId);
            if (chunkedMsgIds != null && chunkedMsgIds.length > 0) {
                Collections.addAll(messageIds, chunkedMsgIds);
            }
            consumerBase.unAckedChunkedMessageIdSequenceMap.remove((MessageIdImpl) messageId);
        }
    }

    public void clear() {
        writeLock.lock();
        try {
            messageIdPartitionMap.clear();
            timePartitions.forEach(tp -> tp.clear());
        } finally {
            writeLock.unlock();
        }
    }

    public boolean add(MessageId messageId) {
        if (messageId == null) {
            return false;
        }

        writeLock.lock();
        try {
            HashSet<MessageId> partition = timePartitions.peekLast();
            HashSet<MessageId> previousPartition = messageIdPartitionMap.putIfAbsent(messageId, partition);
            if (previousPartition == null) {
                return partition.add(messageId);
            } else {
                return false;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public boolean add(MessageId messageId, int redeliveryCount) {
        return add(messageId);
    }

    boolean isEmpty() {
        readLock.lock();
        try {
            return messageIdPartitionMap.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public boolean remove(MessageId messageId) {
        if (messageId == null) {
            return false;
        }

        writeLock.lock();
        try {
            boolean removed = false;
            HashSet<MessageId> exist = messageIdPartitionMap.remove(messageId);
            if (exist != null) {
                removed = exist.remove(messageId);
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            return messageIdPartitionMap.size();
        } finally {
            readLock.unlock();
        }
    }

    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<Entry<MessageId, HashSet<MessageId>>> iterator = messageIdPartitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<MessageId, HashSet<MessageId>> entry = iterator.next();
                MessageId messageId = entry.getKey();
                if (messageId.compareTo(msgId) <= 0) {
                    entry.getValue().remove(messageId);
                    iterator.remove();
                    removed++;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    private void stop() {
        writeLock.lock();
        try {
            if (timeout != null && !timeout.isCancelled()) {
                timeout.cancel();
                timeout = null;
            }
            this.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        stop();
    }
}
