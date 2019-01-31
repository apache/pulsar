package org.apache.pulsar.io.flume.source;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.flume.FlumeConfig;
import org.apache.pulsar.io.flume.FlumeConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public abstract class AbstractSource<V> extends PushSource<V> {

    private static final Logger log = LoggerFactory
            .getLogger(AbstractSource.class);

    protected Thread thread = null;

    protected volatile boolean running = false;

    protected final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("[{}] parse events has an error", t.getName(), e);
        }
    };

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {

        FlumeConfig flumeConfig = FlumeConfig.load(config);

        FlumeConnector flumeConnector = new FlumeConnector();
        flumeConnector.StartConnector(flumeConfig);

        this.start();

    }

    public abstract V extractValue(String message);

    protected void start() {
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

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
                    Thread.sleep(1000);
                    BlockingQueue<Map<String, Object>> blockingQueue = SinkOfFlume.getQueue();
                    while (!blockingQueue.isEmpty()) {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutput out = null;
                        out = new ObjectOutputStream(bos);
                        Map<String, Object> message = blockingQueue.take();
                        System.out.println(message);
                        System.out.println();
                        out.writeObject(message.get("body"));
                        out.flush();
                        byte[] m = bos.toByteArray();
                        String m1 = new String(m);
                        System.out.println(m1);
                        bos.close();
//                        consume(new FlumeRecord<>());
                    }
                }
            } catch (Exception e) {
                log.error("process error!", e);
            } finally {
            }
        }
    }

    @Getter
    @Setter
    static private class FlumeRecord<V> implements Record<V> {
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
