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
package org.apache.pulsar.functions.source;

import java.util.Map;
import java.util.Optional;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.ProcessingGuarantees;

/**
 * The record returned by the proxy to the user.
 */
@CustomLog
public class PulsarFunctionRecord<T> implements Record<T> {

    private final Record<T> record;
    private final FunctionDetails functionConfig;

    public PulsarFunctionRecord(Record<T> record, FunctionDetails functionConfig) {
        this.record = record;
        this.functionConfig = functionConfig;
    }

    @Override
    public Optional<String> getTopicName() {
        return record.getTopicName();
    }

    @Override
    public Optional<String> getKey() {
        return record.getKey();
    }

    @Override
    public Schema<T> getSchema() {
        return record.getSchema();
    }

    @Override
    public T getValue() {
        return record.getValue();
    }

    @Override
    public Optional<Long> getEventTime() {
        return record.getEventTime();
    }

    @Override
    public Optional<String> getPartitionId() {
        return record.getPartitionId();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return record.getPartitionIndex();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return record.getRecordSequence();
    }

    @Override
    public Map<String, String> getProperties() {
        return record.getProperties();
    }

    @SuppressWarnings("deprecation")
    @Override
    public void ack() {
        ProcessingGuarantees processingGuarantees = functionConfig.getProcessingGuarantees();
        if (processingGuarantees == ProcessingGuarantees.MANUAL) {
            record.ack();
        } else {
            log.warn()
                    .attr("processingGuarantees", processingGuarantees)
                    .attr("autoAck", functionConfig.isAutoAck())
                    .log("Ignore this ack option, the framework will automatically ack");
        }
    }

    @Override
    public void fail() {
        record.fail();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return record.getDestinationTopic();
    }

    @Override
    public Optional<Message<T>> getMessage() {
        return record.getMessage();
    }
}
