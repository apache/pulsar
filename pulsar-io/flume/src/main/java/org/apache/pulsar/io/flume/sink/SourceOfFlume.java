package org.apache.pulsar.io.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;


public class SourceOfFlume extends AbstractPollableSource implements BatchSizeSupported {

    private static final Logger log = LoggerFactory
            .getLogger(SourceOfFlume.class);

    private long batchSize;

    private SourceCounter counter;

    private final List<Event> eventList = new ArrayList<Event>();

    private Map<String, String> headers;

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
    }

    @Override
    public Status doProcess() {
        Event event;
        byte[] eventBody;
        try {
            while (eventList.size() < this.getBatchSize()) {
                BlockingQueue<Map<String, Object>> blockingQueue = StringSink.getQueue();
                while (!blockingQueue.isEmpty()) {
                    Map<String, Object> message = blockingQueue.take();
                    eventBody = message.get("body").toString().getBytes();
                    headers.clear();
                    headers = new HashMap<String, String>(4);
                    event = EventBuilder.withBody(eventBody, headers);
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
            log.error("KafkaSource EXCEPTION, {}", e);
            counter.incrementEventReadOrChannelFail(e);
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

}
