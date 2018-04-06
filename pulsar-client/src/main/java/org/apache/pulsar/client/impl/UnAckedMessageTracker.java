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
import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);
    protected ConcurrentOpenHashSet<MessageId> currentSet;
    protected ConcurrentOpenHashSet<MessageId> oldOpenSet;
    private final ReentrantReadWriteLock readWriteLock;
    protected final Lock readLock;
    private final Lock writeLock;
    private Timeout timeout;

    public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED = new UnAckedMessageTrackerDisabled();

    private static class UnAckedMessageTrackerDisabled extends UnAckedMessageTracker {
        @Override
        public void clear() {
        }

        @Override
        public boolean add(MessageId m) {
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

    public UnAckedMessageTracker() {
        readWriteLock = null;
        readLock = null;
        writeLock = null;
    }

    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        currentSet = new ConcurrentOpenHashSet<MessageId>();
        oldOpenSet = new ConcurrentOpenHashSet<MessageId>();
        readWriteLock = new ReentrantReadWriteLock();
        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();
        start(client, consumerBase, ackTimeoutMillis);
    }

    public void start(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        this.stop();
        timeout = client.timer().newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                if (isAckTimeout()) {
                    log.warn("[{}] {} messages have timed-out", consumerBase, oldOpenSet.size());
                    Set<MessageId> messageIds = new HashSet<>();
                    oldOpenSet.forEach(messageIds::add);
                    oldOpenSet.clear();
                    consumerBase.redeliverUnacknowledgedMessages(messageIds);
                }
                toggle();
                timeout = client.timer().newTimeout(this, ackTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        }, ackTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    void toggle() {
        writeLock.lock();
        try {
            ConcurrentOpenHashSet<MessageId> temp = currentSet;
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

    public boolean add(MessageId m) {
        readLock.lock();
        try {
            oldOpenSet.remove(m);
            return currentSet.add(m);
        } finally {
            readLock.unlock();
        }

    }

    boolean isEmpty() {
        readLock.lock();
        try {
            return currentSet.isEmpty() && oldOpenSet.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public boolean remove(MessageId m) {
        readLock.lock();
        try {
            return currentSet.remove(m) || oldOpenSet.remove(m);
        } finally {
            readLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            return currentSet.size() + oldOpenSet.size();
        } finally {
            readLock.unlock();
        }
    }

    private boolean isAckTimeout() {
        readLock.lock();
        try {
            return !oldOpenSet.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public int removeMessagesTill(MessageId msgId) {
        readLock.lock();
        try {
            int currentSetRemovedMsgCount = currentSet.removeIf(m -> (m.compareTo(msgId) <= 0));
            int oldSetRemovedMsgCount = oldOpenSet.removeIf(m -> (m.compareTo(msgId) <= 0));

            return currentSetRemovedMsgCount + oldSetRemovedMsgCount;
        } finally {
            readLock.unlock();
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
