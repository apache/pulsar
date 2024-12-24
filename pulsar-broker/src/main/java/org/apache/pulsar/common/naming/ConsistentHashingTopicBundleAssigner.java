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

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import org.apache.pulsar.broker.PulsarService;

public class ConsistentHashingTopicBundleAssigner implements TopicBundleAssignmentStrategy {
    private PulsarService pulsar;
    @Override
    public NamespaceBundle findBundle(TopicName topicName, NamespaceBundles namespaceBundles) {
        NamespaceBundle bundle = namespaceBundles.getBundle(getHashCode(topicName.toString()));
        if (topicName.getDomain().equals(TopicDomain.non_persistent)) {
            bundle.setHasNonPersistentTopic(true);
        }
        return bundle;
    }

    @Override
    public long getHashCode(String name) {
        return pulsar.getNamespaceService().getNamespaceBundleFactory().getHashFunc()
                .hashString(name, StandardCharsets.UTF_8)
                .padToLong();
    }

    @Override
    public void init(PulsarService pulsarService) {
        this.pulsar = pulsarService;
    }

}
