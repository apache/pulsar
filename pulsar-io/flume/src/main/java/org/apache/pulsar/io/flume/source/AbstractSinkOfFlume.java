package org.apache.pulsar.io.flume.source;

import org.apache.flume.sink.AbstractSink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractSinkOfFlume extends AbstractSink {

    protected static BlockingQueue<Map<String, Object>> records;

    public static BlockingQueue getQueue() {
        return records;
    }
}
