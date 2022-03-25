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

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.influxdb.v2.InfluxDBSink;
import org.apache.pulsar.io.influxdb.v2.InfluxDBSinkConfig;

/**
 * Delegate for InfluxDB sinks, one for InfluxDB v1, the other for InfluxDB v2.
 */
@Connector(
        name = "influxdb",
        type = IOType.SINK,
        help = "The InfluxDBGenericRecordSink is used for moving messages from Pulsar to InfluxDB.",
        configClass = InfluxDBSinkConfig.class
)
@Slf4j
public class InfluxDBGenericRecordSink implements Sink<GenericRecord> {
    protected Sink<GenericRecord> sink;

    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {
        try {
            val configV2 = InfluxDBSinkConfig.load(map);
            configV2.validate();
            sink = new InfluxDBSink();
        } catch (Exception e) {
            try {
                val configV1 = org.apache.pulsar.io.influxdb.v1.InfluxDBSinkConfig.load(map);
                configV1.validate();
                sink = new org.apache.pulsar.io.influxdb.v1.InfluxDBGenericRecordSink();
            } catch (Exception e1) {
                throw new Exception("For InfluxDB V2: \n" + e.toString() + "\n"
                + "For InfluxDB V1: \n" + e1.toString());
            }
        }
       sink.open(map, sinkContext);
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        sink.write(record);
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }
}
