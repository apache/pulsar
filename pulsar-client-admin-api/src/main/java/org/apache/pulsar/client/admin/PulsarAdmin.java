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
package org.apache.pulsar.client.admin;

import java.io.Closeable;
import org.apache.pulsar.client.admin.utils.DefaultImplementation;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PulsarAdmin extends Closeable {

    /**
     * Get a new builder instance that can used to configure and build a {@link PulsarAdmin} instance.
     *
     * @return the {@link PulsarAdminBuilder}
     *
     */
    static PulsarAdminBuilder builder() {
        return DefaultImplementation.newAdminClientBuilder();
    }

    /**
     * @return the clusters management object
     */
    Clusters clusters();

    /**
     * @return the brokers management object
     */
    Brokers brokers();

    /**
     * @return the tenants management object
     */
    Tenants tenants();

    /**
     * @return the resourcegroups managements object
     */
    ResourceGroups resourcegroups();

    /**
     *
     * @deprecated since 2.0. See {@link #tenants()}
     */
    @Deprecated
    Properties properties();

    /**
     * @return the namespaces management object
     */
    Namespaces namespaces();

    /**
     * @return the topics management object
     */
    Topics topics();

    /**
     * Get the topic policies management object.
     * @return the topic policies management object
     */
    TopicPolicies topicPolicies();

    /**
     * Get the local/global topic policies management object.
     * @return the topic policies management object
     */
    TopicPolicies topicPolicies(boolean isGlobal);

    /**
     * @return the bookies management object
     */
    Bookies bookies();

    /**
     * @return the persistentTopics management object
     * @deprecated Since 2.0. See {@link #topics()}
     */
    @Deprecated
    NonPersistentTopics nonPersistentTopics();

    /**
     * @return the resource quota management object
     */
    ResourceQuotas resourceQuotas();

    /**
     * @return does a looks up for the broker serving the topic
     */
    Lookup lookups();

    /**
     *
     * @return the functions management object
     */
    Functions functions();

    /**
     * @return the sources management object
     * @deprecated in favor of {@link #sources()}
     */
    @Deprecated
    Source source();

    /**
     * @return the sources management object
     */
    Sources sources();

    /**
     * @return the sinks management object
     * @deprecated in favor of {@link #sinks}
     */
    @Deprecated
    Sink sink();

    /**
     * @return the sinks management object
     */
    Sinks sinks();

    /**
     * @return the Worker stats
     */
    Worker worker();

    /**
     * @return the broker statics
     */
    BrokerStats brokerStats();

    /**
     * @return the proxy statics
     */
    ProxyStats proxyStats();

    /**
     * @return the service HTTP URL that is being used
     */
    String getServiceUrl();

    /**
     * @return the schemas
     */
    Schemas schemas();

    /**
     * @return the packages management object
     */
    Packages packages();

    /**
     *
     * @return the transactions management object
     */
    Transactions transactions();

    /**
     * Close the PulsarAdminClient and release all the resources.
     *
     */
    @Override
    void close();
}
