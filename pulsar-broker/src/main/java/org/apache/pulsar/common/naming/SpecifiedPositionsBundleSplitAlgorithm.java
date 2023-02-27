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


import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.namespace.NamespaceService;

/**
 * This algorithm divides the bundle into several parts by the specified positions.
 */
public class SpecifiedPositionsBundleSplitAlgorithm implements NamespaceBundleSplitAlgorithm{
    @Override
    public CompletableFuture<List<Long>> getSplitBoundary(BundleSplitOption bundleSplitOption) {
        NamespaceService service = bundleSplitOption.getService();
        NamespaceBundle bundle = bundleSplitOption.getBundle();
        List<Long> positions = bundleSplitOption.getPositions();
        if (positions == null || positions.size() == 0) {
            throw new IllegalArgumentException("SplitBoundaries can't be empty");
        }
        // sort all positions
        Collections.sort(positions);
        return service.getOwnedTopicListForNamespaceBundle(bundle).thenCompose(topics -> {
            if (topics == null || topics.size() <= 1) {
                return CompletableFuture.completedFuture(null);
            }
            List<Long> splitBoundaries = positions
                    .stream()
                    .filter(position -> position > bundle.getLowerEndpoint() && position < bundle.getUpperEndpoint())
                    .collect(Collectors.toList());

            if (splitBoundaries.size() == 0) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(splitBoundaries);
        });
    }
}