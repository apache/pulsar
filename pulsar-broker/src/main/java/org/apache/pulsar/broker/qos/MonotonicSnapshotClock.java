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

package org.apache.pulsar.broker.qos;

/**
 * An interface representing a clock that provides a monotonic counter in nanoseconds.
 * The counter is guaranteed to be monotonic, ensuring it will always increase or remain constant, but never decrease.
 *
 * Monotonicity ensures the time will always progress forward, making it ideal for measuring elapsed time.
 * The monotonic clock is not related to the wall-clock time and is not affected by changes to the system time.
 * The tick value is only significant when compared to other values obtained from the same clock source
 * and should not be used for other purposes.
 *
 * This interface assumes that the implementation can be implemented in a granular way. This means that the value is
 * advanced in steps of a configurable resolution that snapshots the underlying high precision monotonic clock source
 * value.
 * This design allows for optimizations that can improve performance on platforms where obtaining the value of a
 * platform monotonic clock is relatively expensive.
 */
public interface MonotonicSnapshotClock {
    /**
     * Retrieves the latest snapshot of the tick value of the monotonic clock in nanoseconds.
     *
     * When requestSnapshot is set to true, the method will snapshot the underlying high-precision monotonic clock
     * source so that the latest snapshot value is as accurate as possible. This may be a relatively expensive
     * compared to a non-snapshot request.
     *
     * When requestSnapshot is set to false, the method will return the latest snapshot value which is updated by
     * either a call that requested a snapshot or by an update thread that is configured to update the snapshot value
     * periodically.
     *
     * This method returns a value that is guaranteed to be monotonic, meaning it will always increase or remain the
     * same, never decrease. The returned value is only significant when compared to other values obtained from the same
     * clock source and should not be used for other purposes.
     *
     * @param requestSnapshot If set to true, the method will request a new snapshot from the underlying more
     *                        high-precision monotonic clock.
     * @return The current tick value of the monotonic clock in nanoseconds.
     */
    long getTickNanos(boolean requestSnapshot);
}