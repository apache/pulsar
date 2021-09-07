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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import org.apache.pulsar.broker.ServiceConfiguration;

public interface DispatcherProvider {

    Dispatcher createDispatcher(Consumer consumer, Subscription subscription);

    static DispatcherProvider createDispatcherProvider(ServiceConfiguration serviceConfiguration) {
        Class<?> providerClass;
        try {
            providerClass = Class.forName(serviceConfiguration.getDispatcherProviderClassName());
            Object obj = providerClass.getDeclaredConstructor().newInstance();
            checkArgument(obj instanceof DispatcherProvider,
                    "The factory has to be an instance of " + DispatcherProvider.class.getName());
            return (DispatcherProvider) obj;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

}
