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

import java.util.List;

/**
 * A view of events in a sliding window.
 *
 * @param <T> the type of event that this window contains.
 */
public interface Window<T> {
    /**
     * Gets the list of events in the window.
     * <p>
     * <b>Note: </b> If the number of tuples in windows is huge, invoking {@code get} would
     * load all the tuples into memory and may throw an OOM exception. Use windowing with persistence
     * </p>
     *
     * @return the list of events in the window.
     */
    List<T> get();

    /**
     * Get the list of newly added events in the window since the last time the window was generated.
     *
     * @return the list of newly added events in the window.
     */
    List<T> getNew();

    /**
     * Get the list of events expired from the window since the last time the window was generated.
     *
     * @return the list of events expired from the window.
     */
    List<T> getExpired();

    /**
     * If processing based on event time, returns the window end time based on watermark otherwise
     * returns the window end time based on processing time.
     *
     * @return the window end timestamp
     */
    Long getEndTimestamp();

    /**
     * Returns the window start timestamp. Will return null if the window length is not based on
     * time duration.
     *
     * @return the window start timestamp or null if the window length is not time based
     */
    Long getStartTimestamp();
}
