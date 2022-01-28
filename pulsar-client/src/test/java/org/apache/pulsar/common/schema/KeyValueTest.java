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
package org.apache.pulsar.common.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteBufSchema;
import org.apache.pulsar.client.impl.schema.ByteBufferSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class KeyValueTest {

    private static final Map<Schema, List<Object>> testData = new HashMap() {

        private static final long serialVersionUID = -3081991052949960650L;

        {
            put(BooleanSchema.of(), Arrays.asList(false, true));
            put(StringSchema.utf8(), Arrays.asList("my string"));
            put(ByteSchema.of(), Arrays.asList((byte) 32767, (byte) -32768));
            put(ShortSchema.of(), Arrays.asList((short) 32767, (short) -32768));
            put(IntSchema.of(), Arrays.asList((int) 423412424, (int) -41243432));
            put(LongSchema.of(), Arrays.asList(922337203685477580L, -922337203685477581L));
            put(FloatSchema.of(), Arrays.asList(5678567.12312f, -5678567.12341f));
            put(DoubleSchema.of(), Arrays.asList(5678567.12312d, -5678567.12341d));
            put(BytesSchema.of(), Arrays.asList("my string".getBytes(UTF_8)));
            put(ByteBufferSchema.of(), Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes(UTF_8))));
            put(ByteBufSchema.of(), Arrays.asList(Unpooled.wrappedBuffer("my string".getBytes(UTF_8))));
            put(DateSchema.of(), Arrays.asList(new Date(new java.util.Date().getTime() - 10000), new Date(new java.util.Date().getTime())));
            put(TimeSchema.of(), Arrays.asList(new Time(new java.util.Date().getTime() - 10000), new Time(new java.util.Date().getTime())));
            put(TimestampSchema.of(), Arrays.asList(new Timestamp(new java.util.Date().getTime()), new Timestamp(new java.util.Date().getTime())));
            put(InstantSchema.of(), Arrays.asList(Instant.now(), Instant.now().minusSeconds(60*23L)));
            put(LocalDateSchema.of(), Arrays.asList(LocalDate.now(), LocalDate.now().minusDays(2)));
            put(LocalTimeSchema.of(), Arrays.asList(LocalTime.now(), LocalTime.now().minusHours(2)));
            put(LocalDateTimeSchema.of(), Arrays.asList(LocalDateTime.now(), LocalDateTime.now().minusDays(2), LocalDateTime.now().minusWeeks(10)));
        }
    };

    @DataProvider(name = "schemas")
    public Object[][] schemas() {
        return new Object[][] {
            { testData }
        };
    }

    @Test(dataProvider = "schemas")
    public void testAllSchemas(Map<Schema, List<Object>> schemas) {
        for (Map.Entry<Schema, List<Object>> keyEntry : schemas.entrySet()) {
            for (Map.Entry<Schema, List<Object>> valueEntry : schemas.entrySet()) {
                testEncodeDecodeKeyValue(
                    keyEntry.getKey(),
                    valueEntry.getKey(),
                    keyEntry.getValue(),
                    valueEntry.getValue()
                );
            }
        }
    }

    private <K, V> void testEncodeDecodeKeyValue(Schema<K> keySchema,
                                                 Schema<V> valueSchema,
                                                 List<K> keys,
                                                 List<V> values) {
        for (K key : keys) {
            for (V value : values) {
                byte[] data = KeyValue.encode(
                    key, keySchema,
                    value, valueSchema
                );

                KeyValue<K, V> kv = KeyValue.decode(
                    data,
                    (keyBytes, valueBytes) -> new KeyValue<>(
                        keySchema.decode(keyBytes),
                        valueSchema.decode(valueBytes)
                    )
                );

                assertEquals(kv.getKey(), key);
                assertEquals(kv.getValue(), value);
            }
        }
    }

}
