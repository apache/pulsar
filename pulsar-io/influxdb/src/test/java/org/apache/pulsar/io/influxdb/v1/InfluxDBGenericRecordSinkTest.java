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
package org.apache.pulsar.io.influxdb.v1;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.influxdb.v1.InfluxDBAbstractSink;
import org.apache.pulsar.io.influxdb.v1.InfluxDBBuilder;
import org.apache.pulsar.io.influxdb.v1.InfluxDBGenericRecordSink;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * InfluxDB Sink test
 */
@Slf4j
public class InfluxDBGenericRecordSinkTest {

    private Message<GenericRecord> message;

    /**
     * A Simple class to test InfluxDB class
     */
    @Data
    public static class Cpu {
        private String measurement;
        private String model;
        private int value;
        private Map<String, String> tags;
    }

    @Mock
    private SinkContext mockSinkContext;

    InfluxDB influxDB;

    InfluxDBAbstractSink influxSink;

    private Map<String, Object> configMap = Maps.newHashMap();

    @BeforeMethod
    public void setUp() throws Exception {
        influxSink = new InfluxDBGenericRecordSink();

        configMap.put("influxdbUrl", "http://localhost:8086");
        configMap.put("database", "testDB");
        configMap.put("consistencyLevel", "ONE");
        configMap.put("logLevel", "NONE");
        configMap.put("retentionPolicy", "autogen");
        configMap.put("gzipEnable", "false");
        configMap.put("batchTimeMs", "200");
        configMap.put("batchSize", "1");

        mockSinkContext = mock(SinkContext.class);
        influxSink.influxDBBuilder = mock(InfluxDBBuilder.class);
        influxDB = mock(InfluxDB.class);

        when(influxSink.influxDBBuilder.build(any())).thenReturn(influxDB);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        influxSink.close();
        verify(influxDB, times(1)).close();
    }

    @Test
    public void testOpenAndWrite() throws Exception {
        message = mock(MessageImpl.class);
        GenericSchema<GenericRecord> genericAvroSchema;
        // prepare a cpu Record
        Cpu cpu = new Cpu();
        cpu.setMeasurement("cpu");
        cpu.setModel("lenovo");
        cpu.setValue(10);

        Map<String, String> tags = Maps.newHashMap();
        tags.put("host", "server-1");
        tags.put("region", "us-west");

        cpu.setTags(tags);
        AvroSchema<Cpu> schema = AvroSchema.of(Cpu.class);

        byte[] bytes = schema.encode(cpu);
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(GenericSchemaImpl.of(schema.getSchemaInfo()));

        Record<GenericRecord> record = PulsarRecord.<GenericRecord>builder()
            .message(message)
            .topicName("influx_cpu")
            .build();

        genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        when(message.getValue())
                .thenReturn(genericAvroSchema.decode(bytes));

        log.info("cpu:{}, Message.getValue: {}, record.getValue: {}",
            cpu.toString(),
            message.getValue().toString(),
            record.getValue().toString());

        influxSink.open(configMap, mockSinkContext);

        verify(this.influxDB, times(1)).describeDatabases();
        verify(this.influxDB, times(1)).createDatabase("testDB");

        doAnswer(invocationOnMock -> {
            BatchPoints batchPoints = invocationOnMock.getArgument(0, BatchPoints.class);
            Assert.assertNotNull(batchPoints, "batchPoints should not be null.");
            return null;
        }).when(influxDB).write(any(BatchPoints.class));

        influxSink.write(record);

        Thread.sleep(1000);

        verify(influxDB, times(1)).write(any(BatchPoints.class));
    }
}
