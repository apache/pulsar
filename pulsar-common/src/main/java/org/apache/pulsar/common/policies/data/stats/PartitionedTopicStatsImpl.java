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
package org.apache.pulsar.common.policies.data.stats;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.TopicStats;

/**
 * Statistics for a partitioned topic.
 * This class is not thread-safe.
 */
@SuppressFBWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class PartitionedTopicStatsImpl extends TopicStatsImpl implements PartitionedTopicStats {

    @Getter
    public PartitionedTopicMetadata metadata;

    @Getter
    public Map<String, TopicStats> partitions;

    public PartitionedTopicStatsImpl() {
        super();
        metadata = new PartitionedTopicMetadata();
        partitions = new HashMap<>();
    }

    public PartitionedTopicStatsImpl(PartitionedTopicMetadata metadata) {
        this();
        this.metadata = metadata;
    }

    @Override
    public void reset() {
        super.reset();
        partitions.clear();
        metadata.partitions = 0;
    }

}