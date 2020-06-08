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
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.namespace.NamespaceService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Algorithm interface for namespace bundle split.
 */
public interface NamespaceBundleSplitAlgorithm {

    String rangeEquallyDivideName = "range_equally_divide";
    String topicCountEquallyDivideName = "topic_count_equally_divide";

    List<String> availableAlgorithms = Lists.newArrayList(rangeEquallyDivideName, topicCountEquallyDivideName);

    NamespaceBundleSplitAlgorithm rangeEquallyDivide = new RangeEquallyDivideBundleSplitAlgorithm();
    NamespaceBundleSplitAlgorithm topicCountEquallyDivide = new TopicCountEquallyDivideBundleSplitAlgorithm();

    static NamespaceBundleSplitAlgorithm of(String algorithmName) {
        if (algorithmName == null) {
            return null;
        }
        switch (algorithmName) {
            case  rangeEquallyDivideName:
                return rangeEquallyDivide;
            case topicCountEquallyDivideName:
                return topicCountEquallyDivide;
            default:
                return null;
        }
    }

    CompletableFuture<Long> getSplitBoundary(NamespaceService service, NamespaceBundle bundle);
}
