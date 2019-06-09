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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simple abstract class for InfluxDB sink
 */
@Slf4j
public abstract class InfluxDBAbstractSink<T> implements Sink<T> {

    private InfluxDBSinkConfig influxDBSinkConfig;
    private InfluxDB influxDB;
    private InfluxDB.ConsistencyLevel consistencyLevel;
    private String influxDatabase;
    private String retentionPolicy;

    protected InfluxDBBuilder influxDBBuilder = new InfluxDBBuilderImpl();

    private long batchTimeMs;
    private int batchSize;
    private List<Record<T>> incomingList;
    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        influxDBSinkConfig = InfluxDBSinkConfig.load(config);
        influxDBSinkConfig.validate();

        try {
            consistencyLevel = InfluxDB.ConsistencyLevel.valueOf(influxDBSinkConfig.getConsistencyLevel().toUpperCase());
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

        batchTimeMs = influxDBSinkConfig.getBatchTimeMs();
        batchSize = influxDBSinkConfig.getBatchSize();
        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(() -> flush(), batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(Record<T> record) throws Exception {
        int currentSize;
        synchronized (this) {
            if (null != record) {
                incomingList.add(record);
            }
            currentSize = incomingList.size();
        }

        if (currentSize == batchSize) {
            flushExecutor.submit(() -> flush());
        }
    }

    private void flush() {
        BatchPoints.Builder batchBuilder = BatchPoints
            .database(influxDatabase)
            .retentionPolicy(retentionPolicy)
            .consistency(consistencyLevel);

        List<Record<T>>  toFlushList;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            toFlushList = incomingList;
            incomingList = Lists.newArrayList();
        }

        if (CollectionUtils.isNotEmpty(toFlushList)) {
            for (Record<T> record: toFlushList) {
                try {
                    buildBatch(record, batchBuilder);
                } catch (Exception e) {
                    record.fail();
                    toFlushList.remove(record);
                    log.warn("Record flush thread was exception ", e);
                }
            }
        }

        BatchPoints batch = batchBuilder.build();
        try {
            if (CollectionUtils.isNotEmpty(batch.getPoints())) {
                influxDB.write(batch);
            }
            toFlushList.forEach(tRecord -> tRecord.ack());
            batch.getPoints().clear();
            toFlushList.clear();
        } catch (Exception e) {
            toFlushList.forEach(tRecord -> tRecord.fail());
            log.error("InfluxDB write batch data exception ", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != influxDB) {
            influxDB.close();
        }

        if (null != flushExecutor) {
            flushExecutor.shutdown();
        }
    }

    // build Point in BatchPoints builder
    public abstract void buildBatch(Record<T> message, BatchPoints.Builder batchBuilder) throws Exception;
}
