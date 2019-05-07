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
package org.apache.pulsar.functions.windowing;

/**
 * Context information that can be used by the eviction policy.
 */
public interface EvictionContext {
    /**
     * Returns the reference time that the eviction policy could use to
     * evict the events. In the case of event time processing, this would be
     * the watermark time.
     *
     * @return the reference time in millis
     */
    Long getReferenceTime();

    /**
     * Returns the sliding count for count based windows.
     *
     * @return the sliding count
     */
    Long getSlidingCount();


    /**
     * Returns the sliding interval for time based windows.
     *
     * @return the sliding interval
     */
    Long getSlidingInterval();

    /**
     * Returns the current count of events in the queue up to the reference time
     * based on which count based evictions can be performed.
     *
     * @return the current count
     */
    Long getCurrentCount();
}
