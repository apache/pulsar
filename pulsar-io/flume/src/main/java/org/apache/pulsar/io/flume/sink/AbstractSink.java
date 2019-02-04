package org.apache.pulsar.io.flume.sink;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.flume.FlumeConfig;
import org.apache.pulsar.io.flume.FlumeConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractSink<T> implements Sink<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);


    public abstract T extractValue(Record<T> record);

    protected static BlockingQueue<Map<String, Object>> records;

    protected FlumeConnector flumeConnector;

    public static BlockingQueue<Map<String, Object>> getQueue() {
        return records;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        records = new LinkedBlockingQueue<Map<String, Object>>();

        FlumeConfig flumeConfig = FlumeConfig.load(config);

        flumeConnector = new FlumeConnector();
        flumeConnector.StartConnector(flumeConfig);
    }

    @Override
    public void write(Record<T> record) {
        try {
            T message = extractValue(record);
            Map<String, Object> m = new HashMap();
            m.put("body", message);
            records.put(m);
            record.ack();
        } catch (InterruptedException e) {
            record.fail();
            LOG.error("error", e);
        }
    }

    @Override
    public void close() {
        if (flumeConnector != null) {
            flumeConnector.stop();
        }
    }
}
