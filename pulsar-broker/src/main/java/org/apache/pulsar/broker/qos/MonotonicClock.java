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
 */
public interface MonotonicClock {
    /**
     * Retrieves the latest snapshot of the tick value of the monotonic clock in nanoseconds.
     *
     * @return The current tick value of the monotonic clock in nanoseconds.
     */
    long getTickNanos();
}