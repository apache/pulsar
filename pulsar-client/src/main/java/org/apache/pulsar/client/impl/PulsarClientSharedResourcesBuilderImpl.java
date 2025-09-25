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

import java.util.HashSet;
import java.util.Set;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.PulsarClientSharedResourcesBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

public class PulsarClientSharedResourcesBuilderImpl implements PulsarClientSharedResourcesBuilder {
    Set<PulsarClientSharedResources.ResourceType> resourceTypes = new HashSet<>();
    ClientConfigurationData clientConfigurationData;

    @Override
    public PulsarClientSharedResourcesBuilder resourceTypes(
            PulsarClientSharedResources.ResourceType... sharedResourceType) {
        for (PulsarClientSharedResources.ResourceType resourceType : sharedResourceType) {
            resourceTypes.add(resourceType);
        }
        return this;
    }

    @Override
    public PulsarClientSharedResourcesBuilder configureResources(ClientBuilder clientBuilder) {
        clientConfigurationData = ((ClientBuilderImpl) clientBuilder.clone()).getClientConfigurationData();
        return this;
    }

    @Override
    public PulsarClientSharedResources build() {
        return new PulsarClientSharedResourcesImpl(resourceTypes, clientConfigurationData != null
                ? clientConfigurationData : new ClientConfigurationData());
    }
}
