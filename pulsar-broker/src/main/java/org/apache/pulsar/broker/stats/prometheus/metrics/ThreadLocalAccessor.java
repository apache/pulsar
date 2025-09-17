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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import org.jspecify.annotations.Nullable;

class ThreadLocalAccessor {

    private final ConcurrentHashMap<LocalData, Boolean> map = new ConcurrentHashMap<>();
    private final FastThreadLocal<LocalData> localData = new FastThreadLocal<>() {

        @Override
        protected LocalData initialValue() {
            LocalData localData = new LocalData(Thread.currentThread());
            map.put(localData, Boolean.TRUE);
            return localData;
        }

        @Override
        protected void onRemoval(LocalData value) {
            map.remove(value);
        }
    };

    void record(DoublesUnion aggregateSuccess, @Nullable DoublesUnion aggregateFail) {
        map.keySet().forEach(key -> {
            key.record(aggregateSuccess, aggregateFail);
            if (key.shouldRemove()) {
                map.remove(key);
            }
        });
    }

    LocalData getLocalData() {
        return localData.get();
    }

    @VisibleForTesting
    int getLocalDataCount() {
        return map.keySet().size();
    }

    static class LocalData {

        private final DoublesSketch successSketch = new DoublesSketchBuilder().build();
        private final DoublesSketch failSketch = new DoublesSketchBuilder().build();
        private final StampedLock lock = new StampedLock();
        // Keep a weak reference to the owner thread so that we can remove the LocalData when the thread
        // is not alive anymore or has been garbage collected.
        // This reference isn't needed when the owner thread is a FastThreadLocalThread and will be null in that case.
        // The removal is handled by FastThreadLocal#onRemoval when the owner thread is a FastThreadLocalThread.
        private final WeakReference<Thread> ownerThreadReference;

        LocalData(Thread ownerThread) {
            if (ownerThread instanceof FastThreadLocalThread) {
                ownerThreadReference = null;
            } else {
                ownerThreadReference = new WeakReference<>(ownerThread);
            }
        }

        private boolean shouldRemove() {
            if (ownerThreadReference == null) {
                // the owner is a FastThreadLocalThread which handles the removal using FastThreadLocal#onRemoval
                return false;
            } else {
                Thread ownerThread = ownerThreadReference.get();
                if (ownerThread == null) {
                    // the thread has already been garbage collected, LocalData should be removed
                    return true;
                } else {
                    // the thread isn't alive anymore, LocalData should be removed
                    return !ownerThread.isAlive();
                }
            }
        }

        void record(DoublesUnion aggregateSuccess, @Nullable DoublesUnion aggregateFail) {
            long stamp = lock.writeLock();
            try {
                aggregateSuccess.update(successSketch);
                successSketch.reset();
                if (aggregateFail != null) {
                    aggregateFail.update(failSketch);
                    failSketch.reset();
                }
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        void updateSuccess(double value) {
            long stamp = lock.readLock();
            try {
                successSketch.update(value);
            } finally {
                lock.unlockRead(stamp);
            }
        }

        void updateFail(double value) {
            long stamp = lock.readLock();
            try {
                failSketch.update(value);
            } finally {
                lock.unlockRead(stamp);
            }
        }
    }
}
