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
package org.apache.pulsar.common.naming;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.namespace.NamespaceService;

/**
 * This algorithm divides the bundle into two parts with the same topics count.
 */
public class TopicCountEquallyDivideBundleSplitAlgorithm implements NamespaceBundleSplitAlgorithm  {

    @Override
    public CompletableFuture<Long> getSplitBoundary(NamespaceService service, NamespaceBundle bundle) {
        return service.getOwnedTopicListForNamespaceBundle(bundle).thenCompose(topics -> {
            if (topics == null || topics.size() <= 1) {
                return CompletableFuture.completedFuture(null);
            }
            List<Long> topicNameHashList = new ArrayList<>(topics.size());
            for (String topic : topics) {
                topicNameHashList.add(bundle.getNamespaceBundleFactory().getLongHashCode(topic));
            }
            Collections.sort(topicNameHashList);
            long splitStart = topicNameHashList.get(Math.max((topicNameHashList.size() / 2) - 1, 0));
            long splitEnd = topicNameHashList.get(topicNameHashList.size() / 2);
            long splitMiddle = splitStart + (splitEnd - splitStart) / 2;
            return CompletableFuture.completedFuture(splitMiddle);
        });
    }
}
