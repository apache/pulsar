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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceOfFlume extends AbstractPollableSource implements BatchSizeSupported {

    private static final Logger log = LoggerFactory
            .getLogger(SourceOfFlume.class);

    public static final String BATCH_DURATION_MS = "batchDurationMillis";

    private long batchSize;

    private int maxBatchDurationMillis;

    private SourceCounter counter;

    private final List<Event> eventList = new ArrayList<Event>();

    @Override
    public synchronized void doStart() {
        log.info("start source of flume ...");
        this.counter = new SourceCounter("flume-source");
        this.counter.start();
    }

    @Override
    public void doStop() {
        log.info("stop source of flume ...");
        this.counter.stop();
    }

    @Override
    public void doConfigure(Context context) {
        batchSize = context.getInteger(SpoolDirectorySourceConfigurationConstants.BATCH_SIZE, 1000);
        maxBatchDurationMillis = context.getInteger(BATCH_DURATION_MS, 1000);
        log.info("context: {}", context);
    }

    @Override
    public Status doProcess() {
        Event event;
        String eventBody;
        try {
            final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

            while (eventList.size() < this.getBatchSize()
                    && System.currentTimeMillis() < maxBatchEndTime) {
                BlockingQueue<Object> blockingQueue = StringSink.getQueue();
                while (blockingQueue != null && !blockingQueue.isEmpty()) {
                    Object message = blockingQueue.take();
                    eventBody = message.toString();
                    event = EventBuilder.withBody(eventBody.getBytes());
                    eventList.add(event);
                }
            }
            if (eventList.size() > 0) {
                counter.addToEventReceivedCount(eventList.size());
                getChannelProcessor().processEventBatch(eventList);
                eventList.clear();
                return Status.READY;
            }
            return Status.BACKOFF;

        } catch (Exception e) {
            log.error("Flume Source EXCEPTION", e);
            counter.incrementEventReadOrChannelFail(e);
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

}
