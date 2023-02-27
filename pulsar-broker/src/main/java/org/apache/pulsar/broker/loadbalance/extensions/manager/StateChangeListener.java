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
package org.apache.pulsar.broker.loadbalance.extensions.manager;

import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;

public interface StateChangeListener {

    /**
     * Stages of events currently supported.
     * before starting the event/successful completion/failed completion.
     */
    enum EventStage {
        BEFORE,
        SUCCESS,
        FAILURE
    }

    /**
     * Handle the service unit state change.
     *
     * @param serviceUnit - Service Unit(Namespace bundle).
     * @param data - Service unit state data.
     * @param stage - The event stage.
     * @param t - Exception in case of FAILURE, if present/known.
     */
    void handleEvent(String serviceUnit, ServiceUnitStateData data,
                     EventStage stage, Throwable t);
}
