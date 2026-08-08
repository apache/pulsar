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
package org.apache.pulsar.client.api;

import java.util.Optional;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Context passed to authentication providers during initialization.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface AuthenticationInitContext {

    /**
     * Looks up a service by type.
     *
     * @param serviceClass the service type
     * @param <T> the service type
     * @return the service instance, if available
     */
    <T> Optional<T> getService(Class<T> serviceClass);

    /**
     * Looks up a named service by type.
     *
     * @param serviceClass the service type
     * @param name the service name
     * @param <T> the service type
     * @return the named service instance, if available
     */
    <T> Optional<T> getServiceByName(Class<T> serviceClass, String name);
}
