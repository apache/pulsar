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

package org.apache.pulsar.io.kafka.connect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class PulsarKafkaSinkTaskContext implements SinkTaskContext {

    private final Map<String, String> config;
    private final Set<TopicPartition> partitions;
    private final AtomicLong offset = new AtomicLong(0L);

    public PulsarKafkaSinkTaskContext(Map<String, String> config, Set<TopicPartition> partitions) {
        this.config = config;
        this.partitions = partitions;
    }

    @Override
    public Map<String, String> configs() {
        return config;
    }

    public long getAndIncrementOffset() {
        return offset.getAndIncrement();
    }

    public long currentOffset() {
        return offset.get();
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
        map.entrySet().stream().forEach(kv -> offset(kv.getKey(), kv.getValue()));
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        if (partitions.contains(topicPartition)) {
            offset.set(l);
        }
    }

    @Override
    public void timeout(long l) {
        // noop
    }

    @Override
    public Set<TopicPartition> assignment() {
        return partitions;
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
        // noop
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
        // noop
    }

    @Override
    public void requestCommit() {
        // noop
    }
}
