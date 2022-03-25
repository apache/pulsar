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
package org.apache.pulsar.io.flume.source;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.flume.FlumeConfig;
import org.apache.pulsar.io.flume.FlumeConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Simple abstract source class for flume to pulsar.
 */
public abstract class AbstractSource<V> extends PushSource<V> {

    private static final Logger log = LoggerFactory
            .getLogger(AbstractSource.class);

    protected Thread thread = null;

    protected volatile boolean running = false;

    protected final Thread.UncaughtExceptionHandler handler =
            (t, e) -> log.error("[{}] parse events has an error", t.getName(), e);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

        FlumeConfig flumeConfig = FlumeConfig.load(config);

        FlumeConnector flumeConnector = new FlumeConnector();
        flumeConnector.startConnector(flumeConfig);

        this.start();

    }

    public abstract V extractValue(String message);

    protected void start() {
        thread = new Thread(this::process);
        thread.setName("flume source thread");
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        log.info("close flume source");
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join();
        }
    }

    protected void process() {
        while (running) {
            try {
                log.info("start flume receive from sink process");
                while (running) {
                    BlockingQueue<Map<String, Object>> blockingQueue = SinkOfFlume.getQueue();
                    while (blockingQueue != null && !blockingQueue.isEmpty()) {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutput out = null;
                        out = new ObjectOutputStream(bos);
                        Map<String, Object> message = blockingQueue.take();
                        out.writeObject(message.get("body"));
                        out.flush();
                        byte[] m = bos.toByteArray();
                        String m1 = new String(m);
                        bos.close();
                        FlumeRecord flumeRecord = new FlumeRecord<>();
                        flumeRecord.setRecord(extractValue(m1));
                        consume(flumeRecord);
                    }
                }
            } catch (Exception e) {
                log.error("process error!", e);
            }
        }
    }

    @Getter
    @Setter
    private static class FlumeRecord<V> implements Record<V> {
        private V record;
        private Long id;

        @Override
        public Optional<String> getKey() {
            return Optional.of(Long.toString(id));
        }

        @Override
        public V getValue() {
            return record;
        }
    }

}
