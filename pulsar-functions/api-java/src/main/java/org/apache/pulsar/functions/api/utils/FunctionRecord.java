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
package org.apache.pulsar.functions.api.utils;

import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;

@Builder(builderMethodName = "")
public class FunctionRecord<T> implements Record<T> {

    private final T value;
    private final String topicName;
    private final String destinationTopic;
    private final Map<String, String> properties;
    private final String key;
    private final Schema<T> schema;
    private final Long eventTime;
    private final String partitionId;
    private final Integer partitionIndex;
    private final Long recordSequence;

    public static <T> FunctionRecord.FunctionRecordBuilder<T> from(Context context) {
        Record<?> currentRecord = context.getCurrentRecord();
        FunctionRecordBuilder<T> builder = new FunctionRecordBuilder<T>()
                .destinationTopic(context.getOutputTopic())
                .properties(currentRecord.getProperties());
        currentRecord.getTopicName().ifPresent(builder::topicName);
        currentRecord.getKey().ifPresent(builder::key);
        currentRecord.getEventTime().ifPresent(builder::eventTime);
        currentRecord.getPartitionId().ifPresent(builder::partitionId);
        currentRecord.getPartitionIndex().ifPresent(builder::partitionIndex);
        currentRecord.getRecordSequence().ifPresent(builder::recordSequence);

        // TODO: add message

        return builder;
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Optional<String> getTopicName() {
        return Optional.ofNullable(topicName);
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.ofNullable(destinationTopic);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }

    @Override
    public Schema<T> getSchema() {
        return schema;
    }

    @Override
    public Optional<Long> getEventTime() {
        return Optional.ofNullable(eventTime);
    }

    @Override
    public Optional<String> getPartitionId() {
        return Optional.ofNullable(partitionId);
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return Optional.ofNullable(partitionIndex);
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return Optional.ofNullable(recordSequence);
    }

}
