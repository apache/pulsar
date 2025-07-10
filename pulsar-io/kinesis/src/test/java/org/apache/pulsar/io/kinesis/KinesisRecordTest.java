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

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.EncryptionType;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class KinesisRecordTest {

    private KinesisClientRecord mockRecord;
    private final String shardId = "shard-001";
    private final long millisBehindLatest = 12345L;
    private final String partitionKey = "test-key";
    private final String sequenceNumber = "seq-123";
    private final Instant arrivalTimestamp = Instant.now();

    @BeforeMethod
    public void setup() {
        mockRecord = Mockito.mock(KinesisClientRecord.class);
        when(mockRecord.partitionKey()).thenReturn(partitionKey);
        when(mockRecord.sequenceNumber()).thenReturn(sequenceNumber);
        when(mockRecord.approximateArrivalTimestamp()).thenReturn(arrivalTimestamp);
        when(mockRecord.encryptionType()).thenReturn(EncryptionType.NONE);
        when(mockRecord.data()).thenReturn(ByteBuffer.wrap("test-data".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testAllPropertiesIncluded() {
        Set<String> propertiesToInclude = new HashSet<>(Arrays.asList(
                KinesisRecord.ARRIVAL_TIMESTAMP,
                KinesisRecord.ENCRYPTION_TYPE,
                KinesisRecord.PARTITION_KEY,
                KinesisRecord.SEQUENCE_NUMBER,
                KinesisRecord.SHARD_ID,
                KinesisRecord.MILLIS_BEHIND_LATEST
        ));

        KinesisRecord kinesisRecord = new KinesisRecord(mockRecord, shardId, millisBehindLatest, propertiesToInclude, null);
        Map<String, String> properties = kinesisRecord.getProperties();

        assertEquals(properties.size(), 6);
        assertEquals(properties.get(KinesisRecord.SHARD_ID), shardId);
        assertEquals(properties.get(KinesisRecord.MILLIS_BEHIND_LATEST), String.valueOf(millisBehindLatest));
        assertEquals(properties.get(KinesisRecord.PARTITION_KEY), partitionKey);
        assertEquals(properties.get(KinesisRecord.SEQUENCE_NUMBER), sequenceNumber);
        assertEquals(properties.get(KinesisRecord.ARRIVAL_TIMESTAMP), arrivalTimestamp.toString());
        assertEquals(properties.get(KinesisRecord.ENCRYPTION_TYPE), EncryptionType.NONE.toString());
    }

    @Test
    public void testSomePropertiesIncluded() {
        Set<String> propertiesToInclude = new HashSet<>(Arrays.asList(
                KinesisRecord.SHARD_ID,
                KinesisRecord.SEQUENCE_NUMBER
        ));

        KinesisRecord kinesisRecord = new KinesisRecord(mockRecord, shardId, millisBehindLatest, propertiesToInclude, null);
        Map<String, String> properties = kinesisRecord.getProperties();

        assertEquals(properties.size(), 2);
        assertTrue(properties.containsKey(KinesisRecord.SHARD_ID));
        assertTrue(properties.containsKey(KinesisRecord.SEQUENCE_NUMBER));

        assertFalse(properties.containsKey(KinesisRecord.PARTITION_KEY));
        assertFalse(properties.containsKey(KinesisRecord.ARRIVAL_TIMESTAMP));
        assertFalse(properties.containsKey(KinesisRecord.ENCRYPTION_TYPE));
        assertFalse(properties.containsKey(KinesisRecord.MILLIS_BEHIND_LATEST));
    }

    @Test
    public void testNoPropertiesIncluded() {
        Set<String> propertiesToInclude = Collections.emptySet();

        KinesisRecord kinesisRecord = new KinesisRecord(mockRecord, shardId, millisBehindLatest, propertiesToInclude, null);
        Map<String, String> properties = kinesisRecord.getProperties();

        assertTrue(properties.isEmpty());
    }
}