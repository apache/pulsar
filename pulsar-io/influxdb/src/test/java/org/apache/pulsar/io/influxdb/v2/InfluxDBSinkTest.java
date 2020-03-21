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
package org.apache.pulsar.io.influxdb.v2;

import com.google.common.collect.Maps;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.write.Point;
import lombok.Data;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.StructSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class InfluxDBSinkTest {
    @Data
    public static class Cpu {
        private String measurement;
        private long timestamp;
        private Map<String, String> tags;

        @org.apache.avro.reflect.AvroSchema("{\"type\": \"map\", \"values\": " +
                "[\"string\", \"int\", \"bytes\", \"long\",\"float\", \"double\", \"boolean\"]}")
        private Map<String, Object> fields;
    }
    private Cpu cpu;

    InfluxDBSink influxSink;
    InfluxDBClient influxDBClient;
    WriteApi writeApi;

    private Long timestamp;

    @Before
    public void setUp() throws Exception {
        // prepare a cpu Record
        cpu = new Cpu();
        cpu.setMeasurement("cpu");
        timestamp = Instant.now().toEpochMilli();
        cpu.timestamp = timestamp;
        cpu.tags = Maps.newHashMap();
        cpu.tags.put("host", "server-1");
        cpu.tags.put("region", "us-west");
        cpu.fields = Maps.newHashMap();
        cpu.fields.put("model", "lenovo");
        cpu.fields.put("value", 10);

        influxSink = new InfluxDBSink();
        influxSink.influxDBClientBuilder = mock(InfluxDBClientBuilder.class);
        influxDBClient = mock(InfluxDBClient.class);
        writeApi = mock(WriteApi.class);

        when(influxSink.influxDBClientBuilder.build(any())).thenReturn(influxDBClient);
        when(influxDBClient.getWriteApi(any())).thenReturn(writeApi);
    }

    @Test
    public void testJsonSchema() {
        JSONSchema<Cpu> schema = JSONSchema.of(Cpu.class);

        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(GenericSchemaImpl.of(schema.getSchemaInfo()));
        GenericSchema<GenericRecord> genericSchema = GenericSchemaImpl.of(autoConsumeSchema.getSchemaInfo());

        assertFalse(genericSchema instanceof GenericAvroSchema);

        byte[] bytes = schema.encode(cpu);
        GenericRecord record = genericSchema.decode(bytes);

        assertEquals("cpu", record.getField("measurement"));

        // FIXME: GenericJsonRecord will parse long to string or int, depends on whether the value is greater than max Integer. We should modify the behavior of GenericJsonSchema
        assertEquals(timestamp+"", record.getField("timestamp"));

        assertEquals("server-1", ((GenericRecord)record.getField("tags")).getField("host"));
        assertEquals(10, ((GenericRecord)record.getField("fields")).getField("value"));
    }

    @Test
    public void testAvroSchema() {
        AvroSchema<Cpu> schema = AvroSchema.of(Cpu.class);

        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(GenericSchemaImpl.of(schema.getSchemaInfo()));
        GenericSchema<GenericRecord> genericAvroSchema = GenericSchemaImpl.of(autoConsumeSchema.getSchemaInfo());

        assertTrue(genericAvroSchema instanceof GenericAvroSchema);

        byte[] bytes = schema.encode(cpu);
        GenericRecord record = genericAvroSchema.decode(bytes);

        assertEquals("cpu", record.getField("measurement"));
        assertEquals(timestamp, record.getField("timestamp"));
        assertEquals("server-1", ((Map)record.getField("tags")).get(new Utf8("host")).toString());
        assertEquals(10, ((Map)record.getField("fields")).get(new Utf8("value")));
    }

    @Test
    public void testOpenWriteCloseAvro() throws Exception {
        AvroSchema<Cpu> avroSchema = AvroSchema.of(Cpu.class);
        openWriteClose(avroSchema);
    }

    @Test
    public void testOpenWriteCloseJson() throws Exception {
        JSONSchema<Cpu> jsonSchema = JSONSchema.of(Cpu.class);
        openWriteClose(jsonSchema);
    }

    private void openWriteClose(StructSchema<Cpu> schema) throws Exception {
        // test open
        Map<String, Object> map = new HashMap();
        map.put("influxdbUrl", "http://localhost:9999");
        map.put("token", "xxxx");
        map.put("organization", "example-org");
        map.put("bucket", "example-bucket");
        map.put("precision", "ns");
        map.put("logLevel", "NONE");

        map.put("gzipEnable", false);
        map.put("batchTimeMs", 1000);
        map.put("batchSize", 5000);
        influxSink.open(map, null);
        verify(influxDBClient, times(1)).getWriteApi(any());

        // test write
        Message<GenericRecord> message = mock(MessageImpl.class);

        GenericSchema<GenericRecord> genericSchema = GenericSchemaImpl.of(schema.getSchemaInfo());
        when(message.getValue())
                .thenReturn(genericSchema.decode(schema.encode(cpu)));

        Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
                .message(message)
                .topicName("influx_cpu")
                .build();

        influxSink.write(record);
        verify(writeApi, times(1)).writePoint(any(Point.class));

        ArgumentCaptor<Point> captor = ArgumentCaptor.forClass(Point.class);
        verify(writeApi).writePoint(captor.capture());
        Point point = captor.getValue();
        assertTrue(point.hasFields());
        assertEquals("ns", point.getPrecision().getValue());
        assertEquals("cpu,host=server-1,region=us-west model=\"lenovo\",value=10i "+timestamp, point.toLineProtocol());

        // test close
        influxSink.close();
        verify(writeApi, times(1)).close();
        verify(influxDBClient, times(1)).close();
    }
}