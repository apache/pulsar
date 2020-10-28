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
import com.influxdb.client.WriteApiBlocking;
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
import java.util.List;
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
    WriteApiBlocking writeApi;

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
        writeApi = mock(WriteApiBlocking.class);

        when(influxSink.influxDBClientBuilder.build(any())).thenReturn(influxDBClient);
        when(influxDBClient.getWriteApiBlocking()).thenReturn(writeApi);
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

        // compare the String type
        assertEquals(timestamp + "", record.getField("timestamp").toString());

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
        map.put("batchTimeMs", 10000);
        map.put("batchSize", 2);
        influxSink.open(map, null);
        verify(influxDBClient, times(1)).getWriteApiBlocking();

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
        verify(writeApi, times(0)).writePoints(anyList());
        influxSink.write(record);
        Thread.sleep(100);
        verify(writeApi, times(1)).writePoints(anyList());

        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(writeApi).writePoints(captor.capture());
        List<Point> points = captor.getValue();
        assertEquals(2, points.size());
        assertTrue(points.get(0).hasFields());
        assertEquals("ns", points.get(0).getPrecision().getValue());
        assertEquals("cpu,host=server-1,region=us-west model=\"lenovo\",value=10i "+timestamp, points.get(0).toLineProtocol());

        // test close
        influxSink.close();
        verify(influxDBClient, times(1)).close();
    }
}