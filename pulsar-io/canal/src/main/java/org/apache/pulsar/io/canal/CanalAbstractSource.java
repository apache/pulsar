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
package org.apache.pulsar.io.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


/**
 * A Simple abstract class for mysql binlog sync to pulsar.
 */
@Slf4j
public abstract class CanalAbstractSource<V> extends PushSource<V> {

    protected Thread thread = null;

    protected volatile boolean running = false;

    private CanalConnector connector;

    private CanalSourceConfig canalSourceConfig;

    private static final String DESTINATION = "destination";

    protected final Thread.UncaughtExceptionHandler handler = (t, e) -> log.error("[{}] parse events has an error", t.getName(), e);

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        canalSourceConfig = CanalSourceConfig.load(config);
        if (canalSourceConfig.getCluster()) {
            connector = CanalConnectors.newClusterConnector(canalSourceConfig.getZkServers(),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
            log.info("Start canal connect in cluster mode, canal cluster info {}", canalSourceConfig.getZkServers());
        } else {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(canalSourceConfig.getSingleHostname(), canalSourceConfig.getSinglePort()),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
            log.info("Start canal connect in standalone mode, canal server info {}:{}",
                    canalSourceConfig.getSingleHostname(), canalSourceConfig.getSinglePort());
        }
        log.info("canal source destination {}", canalSourceConfig.getDestination());
        this.start();

    }

    protected void start() {
        Objects.requireNonNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setName("canal source thread");
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        log.info("close canal source");
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join();
        }
        if (connector != null) {
            connector.disconnect();
        }

        MDC.remove(DESTINATION);
    }

    protected void process() {
        while (running) {
            try {
                MDC.put(DESTINATION, canalSourceConfig.getDestination());
                connector.connect();
                log.info("start canal process");
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(canalSourceConfig.getBatchSize());
                    message.setRaw(false);
                    List<FlatMessage> flatMessages = MessageUtils.messageConverter(message);
                    long batchId = getMessageId(message);
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        if (flatMessages != null) {
                            CanalRecord<V> canalRecord = new CanalRecord<>(connector);
                            canalRecord.setId(batchId);
                            canalRecord.setRecord(extractValue(flatMessages));
                            consume(canalRecord);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove(DESTINATION);
            }
        }
    }

    public abstract Long getMessageId(Message message);

    public abstract V extractValue(List<FlatMessage> flatMessages);

    @Getter
    @Setter
    private static class CanalRecord<V> implements Record<V> {

        private V record;
        private Long id;
        private CanalConnector connector;

        public CanalRecord(CanalConnector connector) {
            this.connector = connector;
        }

        @Override
        public Optional<String> getKey() {
            return Optional.of(Long.toString(id));
        }

        @Override
        public V getValue() {
            return record;
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return Optional.of(id);
        }

        @Override
        public void ack() {
            log.info("CanalRecord ack id is {}", this.id);
            connector.ack(this.id);
        }

    }
}