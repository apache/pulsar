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
 * Eviction policy tracks events and decides whether
 * an event should be evicted from the window or not.
 *
 * @param <T> the type of event that is tracked.
 * @param <S> the type of state that is used
 */
public interface EvictionPolicy<T, S> {
    /**
     * The action to be taken when {@link EvictionPolicy#evict(Event)} is invoked.
     */
    enum Action {
        /**
         * expire the event and remove it from the queue.
         */
        EXPIRE,
        /**
         * process the event in the current window of events.
         */
        PROCESS,
        /**
         * don't include in the current window but keep the event
         * in the queue for evaluating as a part of future windows.
         */
        KEEP,
        /**
         * stop processing the queue, there cannot be anymore events
         * satisfying the eviction policy.
         */
        STOP
    }

    /**
     * Decides if an event should be expired from the window, processed in the current
     * window or kept for later processing.
     *
     * @param event the input event
     * @return the {@link EvictionPolicy.Action} to be taken based on the input event
     */
    Action evict(Event<T> event);

    /**
     * Tracks the event to later decide whether
     * {@link EvictionPolicy#evict(Event)} should evict it or not.
     *
     * @param event the input event to be tracked
     */
    void track(Event<T> event);

    /**
     * Sets a context in the eviction policy that can be used while evicting the events.
     * E.g. For TimeEvictionPolicy, this could be used to set the reference timestamp.
     *
     * @param context the eviction context
     */
    void setContext(EvictionContext context);

    /**
     * Returns the current context that is part of this eviction policy.
     *
     * @return the eviction context
     */
    EvictionContext getContext();

    /**
     * Resets the eviction policy.
     */
    void reset();

    /**
     * Return runtime state to be checkpointed by the framework for restoring the eviction policy
     * in case of failures.
     *
     * @return the state
     */
    S getState();

    /**
     * Restore the eviction policy from the state that was earlier checkpointed by the framework.
     *
     * @param state the state
     */
    void restoreState(S state);
}
