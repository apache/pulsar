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
 * Interface for a clock source that returns a monotonic time in nanoseconds with a required precision.
 */
public interface MonotonicClockSource {
    /**
     * Returns the current monotonic clock time in nanoseconds.
     *
     * @param highPrecision if true, the returned value must be a high precision monotonic time in nanoseconds.
     *                      if false, the returned value can be a granular precision monotonic time in nanoseconds.
     * @return the current monotonic clock time in nanoseconds
     */
    long getNanos(boolean highPrecision);
}
