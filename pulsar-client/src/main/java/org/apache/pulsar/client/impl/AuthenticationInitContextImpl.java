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

import io.netty.channel.EventLoopGroup;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.AuthenticationInitContext;

public class AuthenticationInitContextImpl implements AuthenticationInitContext {
    private final Map<Class<?>, Object> sharedServices = new HashMap<>();
    private final Map<String, Map<Class<?>, Object>> namedServices = new HashMap<>();

    public AuthenticationInitContextImpl(EventLoopGroup eventLoopGroup,
                                         Timer timer,
                                         NameResolver<InetAddress> nameResolver) {
        this.sharedServices.put(EventLoopGroup.class, eventLoopGroup);
        this.sharedServices.put(Timer.class, timer);
        this.sharedServices.put(NameResolver.class, nameResolver);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getService(Class<T> serviceClass) {
        if (serviceClass == null) {
            throw new IllegalArgumentException("Service class cannot be null");
        }
        return Optional.ofNullable((T) sharedServices.get(serviceClass));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getServiceByName(Class<T> serviceClass, String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Service name cannot be null or empty");
        }
        Map<Class<?>, Object> services = namedServices.get(name);
        if (services != null) {
            return Optional.ofNullable((T) services.get(serviceClass));
        }
        return Optional.empty();
    }

    public <T> void addService(Class<T> serviceClass, T instance) {
        sharedServices.put(serviceClass, instance);
    }

    public <T> void addService(Class<T> serviceClass, String name, T instance) {
        namedServices.computeIfAbsent(name, k -> new HashMap<>())
                .put(serviceClass, instance);
    }
}
