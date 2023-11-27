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

package org.apache.pulsar.broker.service;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class ThrottleTracker {
    public static final long PAUSE_DEDUPLICATION_RESOLUTION = TimeUnit.MILLISECONDS.toNanos(10);
    private static final AtomicIntegerFieldUpdater<ThrottleTracker> THROTTLE_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(
                    ThrottleTracker.class, "throttleCount");

    private static final AtomicLongFieldUpdater<ThrottleTracker> MAX_END_OF_THROTTLE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(
                    ThrottleTracker.class, "maxEndOfThrottleNanos");
    private volatile int throttleCount;
    private volatile long maxEndOfThrottleNanos = 0L;
    private final Supplier<ChannelHandlerContext> ctxSupplier;
    private final AtomicIntegerFlags throttlingFlags = new AtomicIntegerFlags();

    private final LongSupplier clockSource;


    public ThrottleTracker(Supplier<ChannelHandlerContext> ctxSupplier, LongSupplier clockSource) {
        this.ctxSupplier = ctxSupplier;
        this.clockSource = clockSource;
    }

    public boolean changeThrottlingFlag(int index, boolean throttlingEnabled) {
        if (throttlingFlags.changeFlag(index, throttlingEnabled)) {
            if (throttlingEnabled) {
                incrementThrottleCount();
            } else {
                decrementThrottleCount();
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean getThrottlingFlag(int index) {
        return throttlingFlags.getFlag(index);
    }

    public void decrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.decrementAndGet(this);
        if (currentThrottleCount == 0) {
            changeAutoRead(true);
        }
    }

    public void incrementThrottleCount() {
        int currentThrottleCount = THROTTLE_COUNT_UPDATER.incrementAndGet(this);
        if (currentThrottleCount == 1) {
            changeAutoRead(false);
        }
    }

    protected void changeAutoRead(boolean autoRead) {
        ChannelHandlerContext ctx = ctxSupplier.get();
        if (ctx != null) {
            ctx.channel().config().setAutoRead(autoRead);
        }
    }
}
