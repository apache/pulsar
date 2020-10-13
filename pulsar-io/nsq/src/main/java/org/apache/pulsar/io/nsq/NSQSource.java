package org.apache.pulsar.io.nsq;

import java.io.IOException;
import java.util.Map;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.io.common.IOConfigUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import com.sproutsocial.nsq.Client;
import com.sproutsocial.nsq.Subscriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
    name = "nsq",
    type = IOType.SOURCE,
    help = "A Simple connector moving messages from an NSQ topic to a Pulsar Topic",
    configClass = NSQSourceConfig.class
)
@Slf4j
public class NSQSource extends PushSource<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(NSQ.class);

    private Subscriber subscriber;

    private Object waitObject;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws IOException {
        NSQSourceConfig nsqSourceConfig = IOConfigUtils.loadWithSecrets(config, NSQSourceConfig.class, sourceContext);
        nsqSourceConfig.validate();

        waitObject = new Object();
        startThread(nsqSourceConfig);
    }

    @Override
    public void close() throws Exception{
        stopThread();
    }

    private void startThread(NSQSourceConfig config) {
        String[] lookupds = new String[config.getLookupds().size()];
        config.getLookupds().toArray(lookupds);
        subscriber = new Subscriber(lookupds);

        Thread runnerThread = new Thread(() -> {
            subscriber.subscribe(config.getTopic(), config.getChannel(), (byte[]data) ->{
                consume(new NSQRecord(data));
            });
            LOG.info("NSQ Consumer started for topic {} with channel {}", config.getTopic(), config.getChannel());
            //wait
            try {
                synchronized (waitObject) {
                    waitObject.wait();
                }
            } catch (Exception e) {
                LOG.info("Got an exception in waitObject");
            }
            LOG.debug("Closing the NSQ connection");
            subscriber.stop();
            Client.getDefaultClient().stop();
            LOG.info("NSQ subscriber stopped");
            LOG.info("NSQ Runner Thread ending");
        });
        runnerThread.setName("NSQSubscriberRunner");
        runnerThread.start();
    }

    private void stopThread() {
        LOG.info("Source closed");
        synchronized (waitObject) {
            waitObject.notify();
        }
    }

    @Data
    static private class NSQRecord implements Record<byte[]> {
        private final byte[] value;
    }
}

