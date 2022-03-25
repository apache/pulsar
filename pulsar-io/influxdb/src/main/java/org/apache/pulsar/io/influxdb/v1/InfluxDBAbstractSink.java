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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.influxdb.BatchSink;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

/**
 * A simple abstract class for InfluxDB sink.
 */
@Slf4j
public abstract class InfluxDBAbstractSink<T> extends BatchSink<Point, T> {

    private InfluxDB influxDB;
    private InfluxDB.ConsistencyLevel consistencyLevel;
    private String influxDatabase;
    private String retentionPolicy;

    protected InfluxDBBuilder influxDBBuilder = new InfluxDBBuilderImpl();

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        InfluxDBSinkConfig influxDBSinkConfig = InfluxDBSinkConfig.load(config);
        influxDBSinkConfig.validate();

        super.init(influxDBSinkConfig.getBatchTimeMs(), influxDBSinkConfig.getBatchSize());

        try {
            consistencyLevel = InfluxDB.ConsistencyLevel.valueOf(
                    influxDBSinkConfig.getConsistencyLevel().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal Consistency Level, valid values are: "
                + Arrays.asList(InfluxDB.ConsistencyLevel.values()));
        }

        influxDatabase = influxDBSinkConfig.getDatabase();
        retentionPolicy = influxDBSinkConfig.getRetentionPolicy();

        influxDB = influxDBBuilder.build(influxDBSinkConfig);

        // create the database if not exists
        List<String> databases = influxDB.describeDatabases();
        if (!databases.contains(influxDatabase)) {
            influxDB.createDatabase(influxDatabase);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != influxDB) {
            influxDB.close();
        }
    }

    @Override
    protected void writePoints(List<Point> points) throws Exception {
        BatchPoints.Builder batchBuilder = BatchPoints
                .database(influxDatabase)
                .retentionPolicy(retentionPolicy)
                .consistency(consistencyLevel);

        points.forEach((batchBuilder::point));
        influxDB.write(batchBuilder.build());
    }
}
