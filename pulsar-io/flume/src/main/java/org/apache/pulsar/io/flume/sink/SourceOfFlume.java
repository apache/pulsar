package org.apache.pulsar.io.flume.sink;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;


import com.google.common.base.Optional;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;


public class SourceOfFlume extends AbstractPollableSource implements BatchSizeSupported {

    private static final Logger log = LoggerFactory
            .getLogger(SourceOfFlume.class);

    public static final String BATCH_DURATION_MS = "batchDurationMillis";

    private long batchSize;

    private int maxBatchDurationMillis;

    private SourceCounter counter;

    private final List<Event> eventList = new ArrayList<Event>();

    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();


    @Override
    public synchronized void doStart() {
        log.info("start source of flume ...");
        this.counter = new SourceCounter("flume-source");
        this.counter.start();
    }

    @Override
    public void doStop () {
        log.info("stop source of flume ...");
        this.counter.stop();
    }

    @Override
    public void doConfigure(Context context) {
        batchSize = context.getInteger(BATCH_SIZE, 1000);
        maxBatchDurationMillis = context.getInteger(BATCH_DURATION_MS, 1000);
        log.info("context: {}", context);
    }

    @Override
    public Status doProcess() {
        Event event;
        String eventBody;
        try {
            final long maxBatchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;

            while (eventList.size() < this.getBatchSize() &&
                    System.currentTimeMillis() < maxBatchEndTime) {
                BlockingQueue<Map<String, Object>> blockingQueue = StringSink.getQueue();
                while (blockingQueue != null && !blockingQueue.isEmpty()) {
                    Map<String, Object> message = blockingQueue.take();
                    eventBody = message.get("body").toString();
                    event = EventBuilder.withBody(eventBody.getBytes());
                    eventList.add(event);
                }
            }
            if (eventList.size() > 0) {
                counter.addToEventReceivedCount((long) eventList.size());
                getChannelProcessor().processEventBatch(eventList);
                eventList.clear();
                return Status.READY;
            }
            return Status.BACKOFF;

        } catch (Exception e) {
            log.error("Flume Source EXCEPTION, {}", e);
            counter.incrementEventReadOrChannelFail(e);
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

}
