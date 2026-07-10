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
package org.apache.pulsar.broker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Default implementation of {@link PulsarResourcesExtended}.
 *
 * <p>This implementation provides the standard topic listing functionality
 * by delegating to the {@link NamespaceService}.</p>
 */
public class DefaultPulsarResourcesExtended implements PulsarResourcesExtended {

    @Getter
    private PulsarService pulsarService;

    @Override
    public CompletableFuture<List<String>> listTopicOfNamespace(NamespaceName namespaceName,
                                                                CommandGetTopicsOfNamespace.Mode mode,
                                                                Map<String, String> properties) {
        return pulsarService.getNamespaceService().getListOfTopics(namespaceName, mode);
    }

    @Override
    public void initialize(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    @Override
    public void close() {
        // No specific resources to close in the default implementation
    }
}
