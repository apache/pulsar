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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TimePartitionedUnAckedMessageTracker extends UnAckedMessageTracker implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TimePartitionedUnAckedMessageTracker.class);

    private LinkedList<ConcurrentOpenHashSet<MessageId>> timePartitions;

    protected final long ackTimeoutMillis;
    protected final long tickDurationInMs;

    protected final ConsumerBase<?> consumerBase;

    protected final Lock readLock;
    protected final Lock writeLock;

    private Timeout timeout;

    public TimePartitionedUnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis, long tickDurationInMs) {

        this.consumerBase = consumerBase;
        if (ackTimeoutMillis < tickDurationInMs) {
            this.ackTimeoutMillis = tickDurationInMs;
        } else {
            this.ackTimeoutMillis = ackTimeoutMillis;
        }
        this.tickDurationInMs = tickDurationInMs;
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.timePartitions = new LinkedList<>();

        int blankPartitions = (int)Math.ceil((double)ackTimeoutMillis / tickDurationInMs);
        for (int i = 0; i < blankPartitions; i++) {
            timePartitions.add(new ConcurrentOpenHashSet<>());
        }

        timeout = client.timer().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                Set<MessageId> messageIds = new HashSet<>();
                writeLock.lock();
                try {
                    ConcurrentOpenHashSet<MessageId> headPartition = timePartitions.removeFirst();
                    if (!headPartition.isEmpty()) {
                        log.warn("[{}] {} messages have timed-out", consumerBase, timePartitions.size());
                        headPartition.forEach(messageIds::add);
                    }
                    timePartitions.addLast(new ConcurrentOpenHashSet<>());
                } finally {
                    writeLock.unlock();
                }
                consumerBase.redeliverUnacknowledgedMessages(messageIds);
                timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
            }
        }, tickDurationInMs, TimeUnit.MILLISECONDS);
    }

    public void clear() {
        writeLock.lock();
        try {
            timePartitions.clear();
        } finally {
            writeLock.unlock();
        }
    }

    void toggle() {

    }

    public boolean add(MessageId messageId) {
        writeLock.lock();
        try {
            return timePartitions.peekLast().add(messageId);
        } finally {
            writeLock.unlock();
        }
    }

    boolean isEmpty() {
        readLock.lock();
        try {
            if (timePartitions.isEmpty()) {
                return true;
            }
            boolean empty = true;
            for (ConcurrentOpenHashSet<MessageId> timePartition : timePartitions) {
                if (!timePartition.isEmpty()) {
                    empty = false;
                    break;
                }
            }
            return empty;
        } finally {
            readLock.unlock();
        }
    }

    public boolean remove(MessageId messageId) {
        writeLock.lock();
        try {
            boolean removed = false;
            for (ConcurrentOpenHashSet<MessageId> partition : timePartitions) {
                if(partition.remove(messageId)) {
                    removed = true;
                }
            }
            return removed;
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            long size = 0;
            for (ConcurrentOpenHashSet<MessageId> partition : timePartitions) {
                size += partition.size();
            }
            return size;
        } finally {
            readLock.unlock();
        }
    }

    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            int removed = 0;
            for (ConcurrentOpenHashSet<MessageId> partition : timePartitions) {
                removed += partition.removeIf(m -> (m.compareTo(msgId) <= 0));
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
