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
package org.apache.pulsar.io.dynamodb;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.pulsar.functions.api.Record;
import software.amazon.awssdk.utils.StringUtils;

/**
 *  This is a direct adaptation of the kinesis record for kcl v1,
 *  with a little branching added for dynamo-specific logic.
 */

@Getter
public class StreamsRecord implements Record<byte[]> {
    public static final String ARRIVAL_TIMESTAMP = "ARRIVAL_TIMESTAMP";
    public static final String ENCRYPTION_TYPE = "ENCRYPTION_TYPE";
    public static final String PARTITION_KEY = "PARTITION_KEY";
    public static final String SEQUENCE_NUMBER = "SEQUENCE_NUMBER";
    public static final String EVENT_NAME = "EVENT_NAME";

    private static final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final Optional<String> key;
    private final byte[] value;
    private final Map<String, String> properties = new HashMap<String, String> ();
    public StreamsRecord(com.amazonaws.services.kinesis.model.Record record) {
        if (record instanceof RecordAdapter) {
            com.amazonaws.services.dynamodbv2.model.Record dynamoRecord = ((RecordAdapter) record).getInternalObject();
            this.key = Optional.of(dynamoRecord.getEventID());
            setProperty(EVENT_NAME, dynamoRecord.getEventName());
            setProperty(SEQUENCE_NUMBER, dynamoRecord.getDynamodb().getSequenceNumber());
        } else {
            this.key = Optional.of(record.getPartitionKey());
            setProperty(ARRIVAL_TIMESTAMP, record.getApproximateArrivalTimestamp().toString());
            setProperty(ENCRYPTION_TYPE, record.getEncryptionType());
            setProperty(PARTITION_KEY, record.getPartitionKey());
            setProperty(SEQUENCE_NUMBER, record.getSequenceNumber());
        }

        if (StringUtils.isBlank(record.getEncryptionType())) {
            String s = null;
            try {
                s = decoder.decode(record.getData()).toString();
            } catch (CharacterCodingException e) {
               // Ignore
            }
            this.value = (s != null) ? s.getBytes() : null;
        } else {
            // Who knows?
            this.value = null;
        }
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }
}
