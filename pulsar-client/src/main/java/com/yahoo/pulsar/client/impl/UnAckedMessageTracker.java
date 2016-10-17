/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashSet;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);
    private ConcurrentOpenHashSet<MessageIdImpl> currentSet = new ConcurrentOpenHashSet<MessageIdImpl>();
    private ConcurrentOpenHashSet<MessageIdImpl> oldOpenSet = new ConcurrentOpenHashSet<MessageIdImpl>();
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();
    private Timeout timeout = null;

    public void start(PulsarClientImpl client, ConsumerBase consumerBase, long ackTimeoutMillis) {
        this.stop();
        timeout = client.timer().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                if (isAckTimeout()) {
                    log.warn("[{}] {} messages have timed-out", consumerBase, oldOpenSet.size());
                    List<MessageIdImpl> messageIds = new ArrayList<>();
                    oldOpenSet.forEach(messageIds::add);
                    oldOpenSet.clear();
                    consumerBase.redeliverUnacknowledgedMessages(messageIds);
                }
                toggle();
                timeout = client.timer().newTimeout(this, ackTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }, ackTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    protected void toggle() {
        writeLock.lock();
        try {
            ConcurrentOpenHashSet<MessageIdImpl> temp = currentSet;
            currentSet = oldOpenSet;
            oldOpenSet = temp;
        } finally {
            writeLock.unlock();
        }
    }

    public void clear() {
        readLock.lock();
        try {
            currentSet.clear();
            oldOpenSet.clear();
        } finally {
            readLock.unlock();
        }
    }

    public boolean add(MessageIdImpl m) {
        readLock.lock();
        try {
            oldOpenSet.remove(m);
            return currentSet.add(m);
        } finally {
            readLock.unlock();
        }

    }

    public boolean isEmpty() {
        readLock.lock();
        try {
            return currentSet.isEmpty() && oldOpenSet.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public boolean remove(MessageIdImpl m) {
        readLock.lock();
        try {
            return currentSet.remove(m) || oldOpenSet.remove(m);
        } finally {
            readLock.unlock();
        }
    }

    public long size() {
        readLock.lock();
        try {
            return currentSet.size() + oldOpenSet.size();
        } finally {
            readLock.unlock();
        }
    }

    public boolean isAckTimeout() {
        readLock.lock();
        try {
            return !oldOpenSet.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public int removeMessagesTill(MessageIdImpl msgId) {
        readLock.lock();
        try {
            int currentSetRemovedMsgCount = currentSet.removeIf(m -> ((m.getLedgerId() < msgId.getLedgerId()
                    || (m.getLedgerId() == msgId.getLedgerId() && m.getEntryId() <= msgId.getEntryId()))
                    && m.getPartitionIndex() == msgId.getPartitionIndex()));
            int oldSetRemovedMsgCount = oldOpenSet.removeIf(m -> ((m.getLedgerId() < msgId.getLedgerId()
                    || (m.getLedgerId() == msgId.getLedgerId() && m.getEntryId() <= msgId.getEntryId()))
                    && m.getPartitionIndex() == msgId.getPartitionIndex()));
            return currentSetRemovedMsgCount + oldSetRemovedMsgCount;
        } finally {
            readLock.unlock();
        }
    }

    public void stop() {
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
