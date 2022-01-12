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
package org.apache.pulsar.io.kinesis;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.functions.api.Record;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class KinesisRecord implements Record<byte[]> {
    public static final String ARRIVAL_TIMESTAMP = "";
    public static final String ENCRYPTION_TYPE = "";
    public static final String PARTITION_KEY = "";
    public static final String SEQUENCE_NUMBER = "";

    private static final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final Optional<String> key;
    private final byte[] value;
    private final HashMap<String, String> userProperties = new HashMap<>();
    public KinesisRecord(KinesisClientRecord record) {
        this.key = Optional.of(record.partitionKey());
        // encryption type can (annoyingly) be null, so we default to NONE
        EncryptionType encType = EncryptionType.NONE;
        if (record.encryptionType() != null) {
            encType = record.encryptionType();
        }
        setProperty(ARRIVAL_TIMESTAMP, record.approximateArrivalTimestamp().toString());
        setProperty(ENCRYPTION_TYPE, encType.toString());
        setProperty(PARTITION_KEY, record.partitionKey());
        setProperty(SEQUENCE_NUMBER, record.sequenceNumber());

        if (encType == EncryptionType.NONE) {
            String s = null;
            try {
                s = decoder.decode(record.data()).toString();
            } catch (CharacterCodingException ignored) {
            }
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

    public Map<String, String> getProperties() {
        return userProperties;
    }

    public void setProperty(String key, String value) {
        userProperties.put(key, value);
    }
}
