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
package org.apache.pulsar.broker.intercept;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;

/**
 * This class provides a mechanism to intercept various API calls
 */
public interface InterceptProvider {

    default TenantsInterceptProvider getTenantInterceptProvider() {
        return new TenantsInterceptProvider() {};
    }

    default NamespacesInterceptProvider getNamespaceInterceptProvider() {
        return new NamespacesInterceptProvider() {};
    }

    default TopicsInterceptProvider getTopicInterceptProvider() {
        return new TopicsInterceptProvider() {};
    }

    default FunctionsInterceptProvider getFunctionsInterceptProvider() {
        return new FunctionsInterceptProvider() {};
    }

    default SourcesInterceptProvider getSourcesInterceptProvider() {
        return new SourcesInterceptProvider() {};
    }

    default SinksInterceptProvider getSinksInterceptProvider() {
        return new SinksInterceptProvider() {};
    }

    /**
     * Perform initialization for the intercept provider
     *
     * @param conf broker config object
     */
    default void initialize(ServiceConfiguration conf, PulsarAdmin pulsarAdmin) throws InterceptException {}
}
