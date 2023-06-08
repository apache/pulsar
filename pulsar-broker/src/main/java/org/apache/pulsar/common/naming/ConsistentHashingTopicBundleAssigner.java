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
package org.apache.pulsar.common.naming;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Charsets;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;

public class ConsistentHashingTopicBundleAssigner implements TopicBundleAssignmentStrategy {
    private NamespaceService namespaceService;
    @Override
    public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
        NamespaceName namespaceName = topicName.getNamespaceObject();
        checkArgument(namespaceName.equals(topicName.getNamespaceObject()));
        long hashCode = namespaceService.getNamespaceBundleFactory().getLongHashCode(topicName.toString());
        NamespaceBundle bundle = namespaceBundles.getBundle(hashCode);
        if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
            bundle.setHasNonPersistentTopic(true);
        }
        return bundle;
    }

    @Override
    public void init(NamespaceService namespaceService, PulsarAdmin pulsarAdmin, ServiceConfiguration configuration) {
        this.namespaceService = namespaceService;
    }
}