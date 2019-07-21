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
package org.apache.pulsar.io.flume.sink;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.flume.FlumeConfig;
import org.apache.pulsar.io.flume.FlumeConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Simple abstract sink class for pulsar to flume.
 */
public abstract class AbstractSink<T> implements Sink<T> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSink.class);


    public abstract T extractValue(Record<T> record);

    protected static BlockingQueue<Object> records;

    protected FlumeConnector flumeConnector;

    public static BlockingQueue<Object> getQueue() {
        return records;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        records = new LinkedBlockingQueue<>();

        FlumeConfig flumeConfig = FlumeConfig.load(config);

        flumeConnector = new FlumeConnector();
        flumeConnector.StartConnector(flumeConfig);
    }

    @Override
    public void write(Record<T> record) {
        try {
            T message = extractValue(record);
            records.put(message);
            record.ack();
        } catch (InterruptedException e) {
            record.fail();
            log.error("error", e);
        }
    }

    @Override
    public void close() {
        if (flumeConnector != null) {
            flumeConnector.stop();
        }
    }
}
