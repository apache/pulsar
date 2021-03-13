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
package org.apache.pulsar.io.influxdb;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.influxdb.v2.InfluxDBSink;
import org.influxdb.InfluxDBIOException;
import org.testng.annotations.Test;


public class InfluxDBGenericRecordSinkTest {

    @Test
    public void openInfluxV1() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("influxdbUrl", "http://localhost:8086");
        map.put("database", "test_db");

        InfluxDBGenericRecordSink sink = new InfluxDBGenericRecordSink();
        try {
            sink.open(map, mock(SinkContext.class));
        } catch (InfluxDBIOException e) {
            // Do nothing
        }
        assertTrue(sink.sink instanceof org.apache.pulsar.io.influxdb.v1.InfluxDBGenericRecordSink);
    }

    @Test
    public void openInfluxV2() throws Exception {
        Map<String, Object> map = new HashMap();
        map.put("influxdbUrl", "http://localhost:9999");
        map.put("token", "xxxx");
        map.put("organization", "example-org");
        map.put("bucket", "example-bucket");

        InfluxDBGenericRecordSink sink = new InfluxDBGenericRecordSink();
        try {
            sink.open(map, mock(SinkContext.class));
        } catch (InfluxDBIOException e) {
            // Do nothing
        }
        assertTrue(sink.sink instanceof InfluxDBSink);
    }

    @Test(expectedExceptions = Exception.class,
            expectedExceptionsMessageRegExp = "For InfluxDB V2:.*")
    public void openInvalidInfluxConfig() throws Exception {
        InfluxDBGenericRecordSink sink = new InfluxDBGenericRecordSink();
        sink.open(new HashMap<>(), mock(SinkContext.class));
    }
}