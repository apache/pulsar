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

package org.apache.pulsar.client.impl.metrics;

import com.google.common.collect.Lists;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.ArrayList;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.pulsar.common.naming.TopicName;

@UtilityClass
public class MetricsUtil {

    // By default, advice to use namespace level aggregation only
    private static final List<AttributeKey<String>> DEFAULT_AGGREGATION_LABELS = Lists.newArrayList(
            AttributeKey.stringKey("pulsar.tenant"),
            AttributeKey.stringKey("pulsar.namespace")
    );

    static List<AttributeKey<?>> getDefaultAggregationLabels(Attributes attrs) {
        List<AttributeKey<?>> res = new ArrayList<>();
        res.addAll(DEFAULT_AGGREGATION_LABELS);
        res.addAll(attrs.asMap().keySet());
        return res;
    }

    static Attributes getTopicAttributes(String topic, Attributes baseAttributes) {
        TopicName tn = TopicName.get(topic);

        AttributesBuilder ab = baseAttributes.toBuilder();
        if (tn.isPartitioned()) {
            ab.put("pulsar.partition", tn.getPartitionIndex());
        }
        ab.put("pulsar.topic", tn.getPartitionedTopicName());
        ab.put("pulsar.namespace", tn.getNamespace());
        ab.put("pulsar.tenant", tn.getTenant());
        return ab.build();
    }
}
