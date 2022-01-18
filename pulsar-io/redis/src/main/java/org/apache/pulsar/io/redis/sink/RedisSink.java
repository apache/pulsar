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
package org.apache.pulsar.io.redis.sink;

import com.google.common.collect.Lists;
import io.lettuce.core.RedisFuture;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.redis.RedisSession;

/**
 * A Simple Redis sink, which stores the key/value records from Pulsar in redis.
 * Note that records from Pulsar with null keys or values will be ignored.
 * This class expects records from Pulsar to have a key and value that are stored as bytes or a string.
 */
@Connector(
    name = "redis",
    type = IOType.SINK,
    help = "A sink connector is used for moving messages from Pulsar to Redis.",
    configClass = RedisSinkConfig.class
)
@Slf4j
public class RedisSink implements Sink<byte[]> {

    private RedisSinkConfig redisSinkConfig;

    private RedisSession redisSession;

    private long batchTimeMs;

    private long operationTimeoutMs;

    private int batchSize;

    private List<Record<byte[]>> incomingList;

    private ScheduledExecutorService flushExecutor;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Open Redis Sink");

        redisSinkConfig = RedisSinkConfig.load(config);
        redisSinkConfig.validate();

        redisSession = RedisSession.create(redisSinkConfig);

        operationTimeoutMs = redisSinkConfig.getOperationTimeout();

        batchTimeMs = redisSinkConfig.getBatchTimeMs();
        batchSize = redisSinkConfig.getBatchSize();
        incomingList = Lists.newArrayList();
        flushExecutor = Executors.newScheduledThreadPool(1);
        flushExecutor.scheduleAtFixedRate(this::flush, batchTimeMs, batchTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void write(Record<byte[]> record) throws Exception {
        int currentSize;
        synchronized (this) {
            incomingList.add(record);
            currentSize = incomingList.size();
        }
        if (currentSize == batchSize) {
            flushExecutor.submit(this::flush);
        }
    }

    @Override
    public void close() throws Exception {
        if (null != redisSession) {
            redisSession.close();
        }

        if (null != flushExecutor) {
            flushExecutor.shutdown();
        }
    }

    private void flush() {
        final Map<byte[], byte[]> recordsToSet = new ConcurrentHashMap<>();
        final List<Record<byte[]>> recordsToFlush;

        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            recordsToFlush = incomingList;
            incomingList = Lists.newArrayList();
        }

        if (CollectionUtils.isNotEmpty(recordsToFlush)) {
            for (Record<byte[]> record: recordsToFlush) {
                try {
                    // use an empty string as key when the key is null
                    String recordKey = record.getKey().isPresent() ? record.getKey().get() : "";
                    byte[] key = recordKey.getBytes(StandardCharsets.UTF_8);
                    byte[] value = record.getValue();
                    recordsToSet.put(key, value);
                } catch (Exception e) {
                    record.fail();
                    recordsToFlush.remove(record);
                    log.warn("Record flush thread was exception ", e);
                }
            }
        }

        try {
            if (recordsToSet.size() > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Calling mset with {} values", recordsToSet.size());
                }

                RedisFuture<?> future = redisSession.asyncCommands().mset(recordsToSet);

                if (!future.await(operationTimeoutMs, TimeUnit.MILLISECONDS) || future.getError() != null) {
                    log.warn("Operation failed with error {} or timeout {} is exceeded", future.getError(),
                            operationTimeoutMs);
                    recordsToFlush.forEach(Record::fail);
                    return;
                }
            }
            recordsToFlush.forEach(Record::ack);
            recordsToSet.clear();
            recordsToFlush.clear();
        } catch (InterruptedException e) {
            recordsToFlush.forEach(Record::fail);
            log.error("Redis mset data interrupted exception ", e);
        }
    }
}
