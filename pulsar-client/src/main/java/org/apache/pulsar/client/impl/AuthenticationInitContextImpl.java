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
package org.apache.pulsar.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.AuthenticationInitContext;

final class AuthenticationInitContextImpl implements AuthenticationInitContext {
    private final Map<Class<?>, Object> services = new HashMap<>();
    private final Map<String, Map<Class<?>, Object>> namedServices = new HashMap<>();

    <T> AuthenticationInitContextImpl addService(Class<T> serviceClass, T service) {
        if (service != null) {
            services.put(serviceClass, service);
        }
        return this;
    }

    <T> AuthenticationInitContextImpl addService(String name, Class<T> serviceClass, T service) {
        if (service != null) {
            namedServices.computeIfAbsent(name, ignored -> new HashMap<>()).put(serviceClass, service);
        }
        return this;
    }

    @Override
    public <T> Optional<T> getService(Class<T> serviceClass) {
        if (serviceClass == null) {
            throw new IllegalArgumentException("serviceClass cannot be null");
        }
        return Optional.ofNullable(serviceClass.cast(services.get(serviceClass)));
    }

    @Override
    public <T> Optional<T> getServiceByName(Class<T> serviceClass, String name) {
        if (serviceClass == null) {
            throw new IllegalArgumentException("serviceClass cannot be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }
        return Optional.ofNullable(namedServices.get(name))
                .map(servicesByName -> serviceClass.cast(servicesByName.get(serviceClass)));
    }
}
