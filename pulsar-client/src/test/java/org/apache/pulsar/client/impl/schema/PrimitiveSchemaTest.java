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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests primitive schemas.
 */
@Slf4j
public class PrimitiveSchemaTest {


    @DataProvider(name = "schemas")
    public Object[][] schemas() {

        // we are not using a static initialization block, see here:
        // https://github.com/apache/pulsar/issues/11037

        final Map<Schema, List<Object>> testData = new HashMap() {
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

        final Map<Schema, List<Object>> testData2 = new HashMap() {
            {
                put(Schema.BOOL, Arrays.asList(false, true));
                put(Schema.STRING, Arrays.asList("my string"));
                put(Schema.INT8, Arrays.asList((byte) 32767, (byte) -32768));
                put(Schema.INT16, Arrays.asList((short) 32767, (short) -32768));
                put(Schema.INT32, Arrays.asList((int) 423412424, (int) -41243432));
                put(Schema.INT64, Arrays.asList(922337203685477580L, -922337203685477581L));
                put(Schema.FLOAT, Arrays.asList(5678567.12312f, -5678567.12341f));
                put(Schema.DOUBLE, Arrays.asList(5678567.12312d, -5678567.12341d));
                put(Schema.BYTES, Arrays.asList("my string".getBytes(UTF_8)));
                put(Schema.BYTEBUFFER, Arrays.asList(ByteBuffer.allocate(10).put("my string".getBytes(UTF_8))));
                put(Schema.DATE, Arrays.asList(new Date(new java.util.Date().getTime() - 10000), new Date(new java.util.Date().getTime())));
                put(Schema.TIME, Arrays.asList(new Time(new java.util.Date().getTime() - 10000), new Time(new java.util.Date().getTime())));
                put(Schema.TIMESTAMP, Arrays.asList(new Timestamp(new java.util.Date().getTime() - 10000), new Timestamp(new java.util.Date().getTime())));
                put(Schema.INSTANT, Arrays.asList(Instant.now(), Instant.now().minusSeconds(60*23L)));
                put(Schema.LOCAL_DATE, Arrays.asList(LocalDate.now(), LocalDate.now().minusDays(2)));
                put(Schema.LOCAL_TIME, Arrays.asList(LocalTime.now(), LocalTime.now().minusHours(2)));
                put(Schema.LOCAL_DATE_TIME, Arrays.asList(LocalDateTime.now(), LocalDateTime.now().minusDays(2), LocalDateTime.now().minusWeeks(10)));
            }
        };

        for (Schema schema : testData.keySet()) {
            assertNotNull(schema);
        }
        for (Schema schema : testData2.keySet()) {
            assertNotNull(schema);
        }

        return new Object[][] { { testData }, { testData2 } };
    }

    @Test(dataProvider = "schemas")
    public void allSchemasShouldSupportNull(Map<Schema, List<Object>> testData) {
        for (Schema<?> schema : testData.keySet()) {
            byte[] bytes = null;
            ByteBuf byteBuf =  null;
            try {
                assertNull(schema.encode(null),
                    "Should support null in " + schema.getSchemaInfo().getName() + " serialization");
                assertNull(schema.decode(bytes),
                    "Should support null in " + schema.getSchemaInfo().getName() + " deserialization");
                assertNull(((AbstractSchema) schema).decode(byteBuf),
                    "Should support null in " + schema.getSchemaInfo().getName() + " deserialization");
            } catch (NullPointerException npe) {
                throw new NullPointerException("NPE when using schema " + schema + " : " + npe.getMessage());
            }
        }
    }

    @Test(dataProvider = "schemas")
    public void allSchemasShouldRoundtripInput(Map<Schema, List<Object>> testData) {
        for (Map.Entry<Schema, List<Object>> test : testData.entrySet()) {
            log.info("Test schema {}", test.getKey());
            for (Object value : test.getValue()) {
                log.info("Encode : {}", value);
                try {
                    assertEquals(value,
                        test.getKey().decode(test.getKey().encode(value)),
                        "Should get the original " + test.getKey().getSchemaInfo().getName() +
                            " after serialization and deserialization");
                } catch (NullPointerException npe) {
                    throw new NullPointerException("NPE when using schema " + test.getKey()
                        + " : " + npe.getMessage());
                }
            }
        }
    }

    @Test
    public void allSchemasShouldHaveSchemaType() {
        assertEquals(SchemaType.BOOLEAN, BooleanSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT8, ByteSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT16, ShortSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT32, IntSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INT64, LongSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.FLOAT, FloatSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.DOUBLE, DoubleSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.STRING, StringSchema.utf8().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, BytesSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, ByteBufferSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.BYTES, ByteBufSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.DATE, DateSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.TIME, TimeSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.TIMESTAMP, TimestampSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.INSTANT, InstantSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.LOCAL_DATE, LocalDateSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.LOCAL_TIME, LocalTimeSchema.of().getSchemaInfo().getType());
        assertEquals(SchemaType.LOCAL_DATE_TIME, LocalDateTimeSchema.of().getSchemaInfo().getType());
    }


}
