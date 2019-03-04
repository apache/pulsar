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

import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;

public class SinkOfFlume extends AbstractSinkOfFlume implements Configurable, BatchSizeSupported {

    private static final Logger LOG = LoggerFactory.getLogger(SinkOfFlume.class);

    private long batchSize;

    private SinkCounter counter = null;

    @Override
    public void configure(Context context) {
        batchSize = context.getInteger(BATCH_SIZE, 1000);
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }


    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;

        try {
            transaction = channel.getTransaction();
            transaction.begin();
            long processedEvents = 0;
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in the channel
                    break;
                }
                if (processedEvents == 0) {
                    result = Status.BACKOFF;
                    counter.incrementBatchEmptyCount();
                } else if (processedEvents < batchSize) {
                    counter.incrementBatchUnderflowCount();
                } else {
                    counter.incrementBatchCompleteCount();
                }
                event.getHeaders();
                event.getBody();
                Map<String, Object> m = new HashMap();
                m.put("headers", event.getHeaders());
                m.put("body", event.getBody());
                records.put(m);
            }
            transaction.commit();
        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            LOG.error("Failed to publish events", ex);
            counter.incrementEventWriteOrChannelFail(ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    // If the transaction wasn't committed before we got the exception, we
                    // need to rollback.
                    transaction.rollback();
                } catch (RuntimeException e) {
                    LOG.error("Transaction rollback failed: " + e.getLocalizedMessage());
                    LOG.debug("Exception follows.", e);
                } finally {
                    transaction.close();
                    transaction = null;
                }
            }
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    @Override
    public synchronized void start() {
        records = new LinkedBlockingQueue<Map<String, Object>>();
        this.counter = new SinkCounter("flume-sink");
    }

    @Override
    public synchronized void stop() {
    }

}
