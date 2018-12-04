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

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WheelTimerUnAckedMessageTracker extends UnAckedMessageTracker implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WheelTimerUnAckedMessageTracker.class);


    protected final HashedWheelTimer wheelTimer;

    protected final Map<MessageId, Timeout> timeoutMap;

    protected final long ackTimeoutMillis;
    protected final long tickDurationInMs;

    protected final ConsumerBase<?> consumerBase;

    protected final Lock readLock;
    protected final Lock writeLock;

    protected AtomicReference<ConcurrentOpenHashSet<MessageId>> paddingSet;

    public static final WheelTimerUnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED = new WheelTimerUnAckedMessageTrackerDisabled();


    private Timeout timeout;

    private static class WheelTimerUnAckedMessageTrackerDisabled extends WheelTimerUnAckedMessageTracker {
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

    public WheelTimerUnAckedMessageTracker() {
        this.wheelTimer = null;
        this.timeoutMap = null;
        this.ackTimeoutMillis = 0L;
        this.tickDurationInMs = 0L;
        this.consumerBase = null;
        this.readLock = null;
        this.writeLock = null;
    }

    public WheelTimerUnAckedMessageTracker(ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        this(consumerBase, ackTimeoutMillis, 1000L);
    }

    public WheelTimerUnAckedMessageTracker(ConsumerBase<?> consumerBase, long ackTimeoutMillis, long tickDurationInMs) {
        this.consumerBase = consumerBase;
        this.ackTimeoutMillis = ackTimeoutMillis;
        this.tickDurationInMs = tickDurationInMs;
        this.timeoutMap = new HashMap<>();
        this.wheelTimer = new HashedWheelTimer(tickDurationInMs , TimeUnit.MILLISECONDS);
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.paddingSet = new AtomicReference<>();
        this.paddingSet.set(new ConcurrentOpenHashSet<>());
        this.wheelTimer.start();
        startTransmit();
    }

    public void startTransmit() {
        timeout = wheelTimer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout t) throws Exception {
                if (paddingSet.get().size() > 0) {
                    log.warn("[{}] {} messages have timed-out", consumerBase, paddingSet.get().size());
                    Set<MessageId> messageIds = new HashSet<>();
                    ConcurrentOpenHashSet<MessageId> toTransmit = paddingSet.getAndSet(new ConcurrentOpenHashSet<>());
                    toTransmit.forEach(messageIds::add);
                    consumerBase.redeliverUnacknowledgedMessages(messageIds);
                }
                timeout = wheelTimer.newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
            }
        }, tickDurationInMs, TimeUnit.MILLISECONDS);
    }

    public void clear() {
        writeLock.lock();
        try {
            timeoutMap.values().forEach(Timeout::cancel);
            timeoutMap.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public boolean add(MessageId messageId) {
        writeLock.lock();
        try {
            Timeout msgTimeout = wheelTimer.newTimeout(timeout -> {
                paddingSet.get().add(messageId);
            }, ackTimeoutMillis, TimeUnit.MILLISECONDS);
            timeoutMap.put(messageId, msgTimeout);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    boolean isEmpty() {
        readLock.lock();
        try {
            return timeoutMap.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public boolean remove(MessageId messageId) {
        writeLock.lock();
        try {
            Timeout timeout = timeoutMap.remove(messageId);
            if (timeout != null) {
                timeout.cancel();
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    long size() {
        readLock.lock();
        try {
            return timeoutMap.size();
        } finally {
            readLock.unlock();
        }
    }

    public int removeMessagesTill(MessageId msgId) {
        writeLock.lock();
        try {
            int removed = 0;
            Iterator<MessageId> iterator = timeoutMap.keySet().iterator();
            while (iterator.hasNext()) {
                MessageId maybeRemove = iterator.next();
                if (maybeRemove.compareTo(msgId) <= 0) {
                    Timeout timeout = timeoutMap.get(maybeRemove);
                    if (timeout != null) {
                        timeout.cancel();
                    }
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
            }
            wheelTimer.stop();
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
