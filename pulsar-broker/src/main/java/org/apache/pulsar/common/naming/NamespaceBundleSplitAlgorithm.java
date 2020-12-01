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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.namespace.NamespaceService;

/**
 * Algorithm interface for namespace bundle split.
 */
public interface NamespaceBundleSplitAlgorithm {

    String RANGE_EQUALLY_DIVIDE_NAME = "range_equally_divide";
    String TOPIC_COUNT_EQUALLY_DIVIDE = "topic_count_equally_divide";

    List<String> AVAILABLE_ALGORITHMS = Lists.newArrayList(RANGE_EQUALLY_DIVIDE_NAME, TOPIC_COUNT_EQUALLY_DIVIDE);

    NamespaceBundleSplitAlgorithm RANGE_EQUALLY_DIVIDE_ALGO = new RangeEquallyDivideBundleSplitAlgorithm();
    NamespaceBundleSplitAlgorithm TOPIC_COUNT_EQUALLY_DIVIDE_ALGO = new TopicCountEquallyDivideBundleSplitAlgorithm();

    static NamespaceBundleSplitAlgorithm of(String algorithmName) {
        if (algorithmName == null) {
            return null;
        }
        switch (algorithmName) {
            case RANGE_EQUALLY_DIVIDE_NAME:
                return RANGE_EQUALLY_DIVIDE_ALGO;
            case TOPIC_COUNT_EQUALLY_DIVIDE:
                return TOPIC_COUNT_EQUALLY_DIVIDE_ALGO;
            default:
                return null;
        }
    }

    CompletableFuture<Long> getSplitBoundary(NamespaceService service, NamespaceBundle bundle);
}
