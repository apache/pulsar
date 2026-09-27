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
package org.apache.pulsar.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.common.util.collections.GrowableArrayBlockingQueue;

/**
 * Shared backing-array capacity budget, measured in element slots rather than live tasks.
 * Only queues created by this group participate; ordinary GrowableArrayBlockingQueues are unaffected.
 */
final class ExecutorQueueTrimmer implements Runnable {
    private static final double HIGH_WATERMARK_HEAP_FRACTION = 0.05;
    private static final double LOW_WATERMARK_HEAP_FRACTION = 0.04;
    private static final int MAX_TRIMS_PER_PASS = 64;
    private final long highWatermark;
    private final long lowWatermark;
    private final Consumer<Runnable> schedule;
    private final ReferenceQueue<GrowableArrayBlockingQueue<?>> collected = new ReferenceQueue<>();
    // Guarded by this. Never acquire queue locks while holding this monitor.
    private final Set<Registration> queues = new HashSet<>();
    private long retainedCapacity;
    private boolean trimming; // Includes both the delay between passes and an executing pass.

    static ExecutorQueueTrimmer create() {
        long maxHeapElements = Runtime.getRuntime().maxMemory() / referenceBytes();
        return new ExecutorQueueTrimmer((long) (maxHeapElements * LOW_WATERMARK_HEAP_FRACTION),
                (long) (maxHeapElements * HIGH_WATERMARK_HEAP_FRACTION),
                task -> CompletableFuture.delayedExecutor(30, TimeUnit.SECONDS, ForkJoinPool.commonPool())
                        .execute(task));
    }

    private static int referenceBytes() {
        try {
            if ("32".equals(System.getProperty("sun.arch.data.model"))) {
                return 4;
            }
            HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            if (bean != null && Boolean.parseBoolean(bean.getVMOption("UseCompressedOops").getValue())) {
                return 4;
            }
        } catch (RuntimeException | LinkageError ignored) {
            // Other JVMs may not expose this option. Conservatively estimate eight-byte references.
        }
        return 8;
    }

    @VisibleForTesting
    ExecutorQueueTrimmer(long lowWatermark, long highWatermark, Consumer<Runnable> schedule) {
        if (lowWatermark < 0 || highWatermark <= lowWatermark) {
            throw new IllegalArgumentException("Expected 0 <= low watermark < high watermark");
        }
        this.highWatermark = highWatermark;
        this.lowWatermark = lowWatermark;
        this.schedule = schedule;
    }

    <T> GrowableArrayBlockingQueue<T> newQueue() {
        return new ManagedQueue<>();
    }

    private final class ManagedQueue<T> extends GrowableArrayBlockingQueue<T> {
        private final Registration registration = register(this);

        @Override
        protected void capacityChanged(int newCapacity) {
            try {
                resized(registration, newCapacity);
            } finally {
                // Do not let reference-queue cleanup race with this queue's final accounting update.
                Reference.reachabilityFence(this);
            }
        }
    }

    private static final class Registration extends WeakReference<GrowableArrayBlockingQueue<?>> {
        private int capacity;

        Registration(GrowableArrayBlockingQueue<?> queue, ReferenceQueue<GrowableArrayBlockingQueue<?>> collected) {
            super(queue, collected);
            capacity = queue.capacity();
        }
    }

    private synchronized Registration register(GrowableArrayBlockingQueue<?> queue) {
        removeCollected();
        Registration registration = new Registration(queue, collected);
        queues.add(registration);
        retainedCapacity += registration.capacity;
        requestPass();
        return registration;
    }

    private synchronized void resized(Registration registration, int capacity) {
        if (!queues.contains(registration)) {
            return;
        }
        retainedCapacity += capacity - registration.capacity;
        registration.capacity = capacity;
        requestPass();
    }

    void unregister(GrowableArrayBlockingQueue<?> queue) {
        Registration registration = null;
        synchronized (this) {
            for (Registration candidate : queues) {
                if (candidate.get() == queue) {
                    registration = candidate;
                    break;
                }
            }
        }
        if (registration == null) {
            return;
        }
        // Called after executor termination, when accepted tasks have drained. Release spare storage
        // before excluding the queue from the budget. Never take queue locks under the registry monitor.
        queue.trim();
        synchronized (this) {
            if (queues.remove(registration)) {
                retainedCapacity -= registration.capacity;
            }
        }
    }

    private void requestPass() {
        if (!trimming && retainedCapacity > highWatermark
                && retainedCapacity > (long) queues.size() * 64) {
            trimming = true;
            schedule.accept(this);
        }
    }

    private void removeCollected() {
        Registration registration;
        while ((registration = (Registration) collected.poll()) != null) {
            if (queues.remove(registration)) {
                retainedCapacity -= registration.capacity;
            }
        }
    }

    @Override
    public void run() {
        List<Registration> snapshot;
        synchronized (this) {
            removeCollected();
            snapshot = new ArrayList<>(queues);
        }
        try {
            // Only weak registrations survive between passes. Occupancy estimates are rechecked by trim().
            List<Candidate> candidates = new ArrayList<>(snapshot.size());
            for (Registration registration : snapshot) {
                candidates.add(new Candidate(registration, reclaimableSlots(registration)));
            }
            // Snapshot the scores: comparing live occupancy can violate the sorting contract.
            candidates.sort(Comparator.comparingLong(Candidate::reclaimableSlots).reversed());
            int trimmed = 0;
            for (Candidate candidate : candidates) {
                if (retainedCapacity() <= lowWatermark || trimmed == MAX_TRIMS_PER_PASS) {
                    break;
                }
                GrowableArrayBlockingQueue<?> queue = candidate.registration().get();
                if (queue != null && queue.trim() > 0) {
                    trimmed++;
                }
            }
        } finally {
            synchronized (this) {
                removeCollected();
                if (retainedCapacity > lowWatermark && retainedCapacity > (long) queues.size() * 64) {
                    // A full or busy queue may become shrinkable after draining. Growth is not required
                    // to trigger this retry. The delay also bounds how often a queue can be trimmed.
                    schedule.accept(this);
                } else {
                    trimming = false;
                }
            }
        }
    }

    private record Candidate(Registration registration, long reclaimableSlots) {
    }

    private static long reclaimableSlots(Registration registration) {
        GrowableArrayBlockingQueue<?> queue = registration.get();
        if (queue == null) {
            return 0;
        }
        int capacity = queue.capacity();
        int size = queue.size();
        if (capacity <= 64 || size > capacity / 4) {
            return 0;
        }
        int target = 64;
        while (target < size * 2) {
            target *= 2;
        }
        return capacity - target;
    }

    @VisibleForTesting
    synchronized long retainedCapacity() {
        return retainedCapacity;
    }

    @VisibleForTesting
    synchronized List<Reference<?>> registrations() {
        return new ArrayList<>(queues);
    }
}
