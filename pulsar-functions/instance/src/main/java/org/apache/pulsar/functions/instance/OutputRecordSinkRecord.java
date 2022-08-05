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
class OutputRecordSinkRecord<T> extends AbstractSinkRecord<T> {

    private final Record<T> sinkRecord;

    OutputRecordSinkRecord(Record<T> sourceRecord, Record<T> sinkRecord) {
        super(sourceRecord);
        this.sinkRecord = sinkRecord;
    }

    @Override
    public Optional<String> getKey() {
        return sinkRecord.getKey();
    }

    @Override
    public T getValue() {
        return sinkRecord.getValue();
    }

    @Override
    public Optional<String> getPartitionId() {
        return sinkRecord.getPartitionId();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return sinkRecord.getPartitionIndex();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return sinkRecord.getRecordSequence();
    }

     @Override
    public Map<String, String> getProperties() {
        return sinkRecord.getProperties();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return sinkRecord.getDestinationTopic();
    }

    @Override
    public Schema<T> getSchema() {
        return getRecordSchema(sinkRecord);
    }

    @Override
    public Optional<Long> getEventTime() {
        return sinkRecord.getEventTime();
    }

    @Override
    public Optional<Message<T>> getMessage() {
        return sinkRecord.getMessage();
    }

    @Override
    public boolean shouldAlwaysSetMessageProperties() {
        return true;
    }
}
