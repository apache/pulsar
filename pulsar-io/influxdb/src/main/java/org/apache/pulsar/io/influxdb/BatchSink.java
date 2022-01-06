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
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;

/**
 * Pulsar sink which can write data to target in batch.
 *
 * @param <T> The type of data to write to target database
 * @param <R> Pulsar message type, such as GenericRecord
 */
@Slf4j
public abstract class BatchSink<T, R> implements Sink<R> {
    private int batchSize;
    private List<Record<R>> incomingList;
    private ScheduledExecutorService flushExecutor;

    protected void init(long batchTimeMs, int batchSize) {
        this.batchSize = batchSize;

        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newSingleThreadScheduledExecutor();
        flushExecutor.scheduleAtFixedRate(this::flush, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public final void write(Record<R> record) {
        int currentSize;
        synchronized (this) {
            if (null != record) {
                incomingList.add(record);
            }
            currentSize = incomingList.size();
        }

        if (currentSize >= batchSize) {
            flushExecutor.submit(this::flush);
        }
    }

    private void flush() {
        List<Record<R>>  toFlushList;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            toFlushList = incomingList;
            incomingList = Lists.newArrayList();
        }

        val points = Lists.<T>newArrayListWithExpectedSize(toFlushList.size());
        if (CollectionUtils.isNotEmpty(toFlushList)) {
            for (Record<R> record: toFlushList) {
                try {
                    points.add(buildPoint(record));
                } catch (Exception e) {
                    record.fail();
                    toFlushList.remove(record);
                    log.warn("Record flush thread was exception ", e);
                }
            }
        }

        try {
            if (CollectionUtils.isNotEmpty(points)) {
                writePoints(points);
            }
            toFlushList.forEach(Record::ack);
            points.clear();
            toFlushList.clear();
        } catch (Exception e) {
            toFlushList.forEach(Record::fail);
            log.error("InfluxDB write batch data exception ", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != flushExecutor) {
            flushExecutor.shutdown();
        }
    }

    protected Object getFiled(GenericRecord record, String fieldName) {
        List<Field> fields = record.getFields();
        val fieldMatch = fields.stream()
                .filter(field -> fieldName.equals(field.getName()))
                .findAny()
                .orElse(null);
        if (null != fieldMatch) {
            return record.getField(fieldMatch);
        } else {
            return null;
        }
    }

    protected abstract T buildPoint(Record<R> message) throws Exception;
    protected abstract void writePoints(List<T> points) throws Exception;
}
