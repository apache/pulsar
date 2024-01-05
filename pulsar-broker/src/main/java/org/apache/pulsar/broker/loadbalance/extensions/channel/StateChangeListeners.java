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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.extensions.manager.StateChangeListener;

@Slf4j
public class StateChangeListeners {

    private final List<StateChangeListener> stateChangeListeners;

    public StateChangeListeners() {
        stateChangeListeners = new CopyOnWriteArrayList<>();
    }

    public void addListener(StateChangeListener listener) {
        Objects.requireNonNull(listener);
        stateChangeListeners.add(listener);
    }

    public void close() {
        this.stateChangeListeners.clear();
    }

    /**
     * Notify all currently added listeners on completion of the future.
     *
     * @return future of a new completion stage
     */
    public <T> CompletableFuture<T> notifyOnCompletion(CompletableFuture<T> future,
                                                       String serviceUnit,
                                                       ServiceUnitStateData data) {
        return notifyOnArrival(serviceUnit, data).
                thenCombine(future, (unused, t) -> t).
                whenComplete((r, ex) -> notify(serviceUnit, data, ex));
    }

    private CompletableFuture<Void> notifyOnArrival(String serviceUnit, ServiceUnitStateData data) {
        stateChangeListeners.forEach(listener -> {
            try {
                listener.beforeEvent(serviceUnit, data);
            } catch (Throwable ex) {
                log.error("StateChangeListener: {} exception while notifying arrival event {} for service unit {}",
                        listener, data, serviceUnit, ex);
            }
        });
        return CompletableFuture.completedFuture(null);
    }

    public void notify(String serviceUnit, ServiceUnitStateData data, Throwable t) {
        stateChangeListeners.forEach(listener -> {
            try {
                listener.handleEvent(serviceUnit, data, t);
            } catch (Throwable ex) {
                log.error("StateChangeListener: {} exception while handling {} for service unit {}",
                        listener, data, serviceUnit, ex);
            }
        });
    }
}
