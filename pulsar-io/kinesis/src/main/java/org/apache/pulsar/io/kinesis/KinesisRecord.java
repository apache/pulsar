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
package org.apache.pulsar.io.kinesis;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.functions.api.Record;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class KinesisRecord implements Record<byte[]> {
    public static final String ARRIVAL_TIMESTAMP = "kinesis.arrival.timestamp";
    public static final String ENCRYPTION_TYPE = "kinesis.encryption.type";
    public static final String PARTITION_KEY = "kinesis.partition.key";
    public static final String SEQUENCE_NUMBER = "kinesis.sequence.number";
    public static final String SHARD_ID = "kinesis.shard.id";
    public static final String MILLIS_BEHIND_LATEST = "kinesis.millis.behind.latest";

    private final Optional<String> key;
    private final byte[] value;
    private final HashMap<String, String> userProperties = new HashMap<>();

    private final String sequenceNumber;
    private final long subSequenceNumber;
    private final KinesisRecordProcessor recordProcessor;

    public KinesisRecord(KinesisClientRecord record, String shardId, long millisBehindLatest,
                         Set<String> propertiesToInclude, KinesisRecordProcessor recordProcessor) {
        this.key = Optional.of(record.partitionKey());
        this.sequenceNumber = record.sequenceNumber();
        this.subSequenceNumber = record.subSequenceNumber();
        this.recordProcessor = recordProcessor;
        // encryption type can (annoyingly) be null, so we default to NONE
        EncryptionType encType = EncryptionType.NONE;
        if (record.encryptionType() != null) {
            encType = record.encryptionType();
        }
        if (propertiesToInclude.contains(ARRIVAL_TIMESTAMP)) {
            setProperty(ARRIVAL_TIMESTAMP, record.approximateArrivalTimestamp().toString());
        }
        if (propertiesToInclude.contains(ENCRYPTION_TYPE)) {
            setProperty(ENCRYPTION_TYPE, encType.toString());
        }
        if (propertiesToInclude.contains(PARTITION_KEY)) {
            setProperty(PARTITION_KEY, record.partitionKey());
        }
        if (propertiesToInclude.contains(SEQUENCE_NUMBER)) {
            setProperty(SEQUENCE_NUMBER, record.sequenceNumber());
        }
        if (propertiesToInclude.contains(SHARD_ID)) {
            setProperty(SHARD_ID, shardId);
        }
        if (propertiesToInclude.contains(MILLIS_BEHIND_LATEST)) {
            setProperty(MILLIS_BEHIND_LATEST, String.valueOf(millisBehindLatest));
        }

        if (encType == EncryptionType.NONE) {
            String s = StandardCharsets.UTF_8.decode(record.data()).toString();
            this.value = (s != null) ? s.getBytes() : null;
        } else if (encType == EncryptionType.KMS) {
            // use the raw encrypted value, let them handle it downstream
            // TODO: support decoding KMS data here... should be fairly simple
            this.value = record.data().array();
        } else {
            // Who knows?
            this.value = null;
        }
    }
    @Override
    public Optional<String> getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public void ack() {
        this.recordProcessor.updateSequenceNumberToCheckpoint(this.sequenceNumber, this.subSequenceNumber);
    }

    @Override
    public void fail() {
        this.recordProcessor.failed();
    }

    public Map<String, String> getProperties() {
        return userProperties;
    }

    public void setProperty(String key, String value) {
        userProperties.put(key, value);
    }
}
