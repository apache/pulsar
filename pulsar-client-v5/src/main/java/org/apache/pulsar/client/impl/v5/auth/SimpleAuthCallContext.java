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
package org.apache.pulsar.client.impl.v5.auth;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;

/**
 * Simple {@link AuthenticationCallContext} implementation backed by a concurrent map of typed
 * per-exchange state objects. Used by the v5&harr;v4 bridges to drive challenge/response exchanges.
 */
class SimpleAuthCallContext implements AuthenticationCallContext {

    private final String brokerHost;
    private final Map<Class<?>, Object> state = new ConcurrentHashMap<>();

    SimpleAuthCallContext(String brokerHost) {
        this.brokerHost = brokerHost;
    }

    @Override
    public String brokerHost() {
        return brokerHost;
    }

    @Override
    public <T> Optional<T> getStateObject(Class<T> clazz) {
        return Optional.ofNullable(clazz.cast(state.get(clazz)));
    }

    @Override
    public <T> void setStateObject(Class<T> clazz, T value) {
        if (value == null) {
            state.remove(clazz);
        } else {
            state.put(clazz, value);
        }
    }
}
