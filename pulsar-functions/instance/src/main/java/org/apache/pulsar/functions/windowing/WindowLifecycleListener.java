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
 * A callback for expiry, activation of events tracked by the {@link WindowManager}
 *
 * @param <T> The type of Event in the window (e.g. Tuple).
 */
public interface WindowLifecycleListener<T> {
    /**
     * Called on expiry of events from the window due to {@link EvictionPolicy}
     *
     * @param events the expired events
     */
    void onExpiry(List<T> events);

    /**
     * Called on activation of the window due to the {@link TriggerPolicy}
     *
     * @param events the list of current events in the window.
     * @param newEvents the newly added events since last activation.
     * @param expired the expired events since last activation.
     * @param referenceTime the reference (event or processing) time that resulted in activation
     */
    default void onActivation(List<T> events, List<T> newEvents, List<T> expired, Long
            referenceTime) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
