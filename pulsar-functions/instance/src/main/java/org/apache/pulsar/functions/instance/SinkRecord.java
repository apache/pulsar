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
package org.apache.pulsar.functions.instance;

import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

@EqualsAndHashCode(callSuper = true)
@ToString
public class SinkRecord<T> extends AbstractSinkRecord<T> {
    private final Record<T> sourceRecord;
    private final T value;

    public SinkRecord(Record<T> sourceRecord, T value) {
        super(sourceRecord);
        this.sourceRecord = sourceRecord;
        this.value = value;
    }

    @Override
    public Optional<String> getKey() {
        return sourceRecord.getKey();
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Optional<String> getPartitionId() {
        return sourceRecord.getPartitionId();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return sourceRecord.getPartitionIndex();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return sourceRecord.getRecordSequence();
    }

    @Override
    public Map<String, String> getProperties() {
        return sourceRecord.getProperties();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return sourceRecord.getDestinationTopic();
    }

    @Override
    public Schema<T> getSchema() {
        return getRecordSchema(sourceRecord);
    }

    @Override
    public Optional<Long> getEventTime() {
        return sourceRecord.getEventTime();
    }

    @Override
    public Optional<Message<T>> getMessage() {
        return sourceRecord.getMessage();
    }

    @Override
    public boolean shouldAlwaysSetMessageProperties() {
        return false;
    }
}
