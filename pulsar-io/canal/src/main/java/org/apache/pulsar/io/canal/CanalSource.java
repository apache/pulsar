package org.apache.pulsar.io.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class CanalSource extends PushSource<Message> {

    protected final static Logger logger = LoggerFactory.getLogger(CanalSource.class);

    protected Thread thread = null;

    protected volatile boolean running = false;

    private CanalConnector connector;

    private CanalSourceConfig canalSourceConfig;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        canalSourceConfig = CanalSourceConfig.load(config);
        if (canalSourceConfig.getCluster()) {
            connector = CanalConnectors.newClusterConnector(canalSourceConfig.getZkServers(),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
        } else {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(canalSourceConfig.getSingleHostname(), canalSourceConfig.getSinglePort()),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
        }
        this.start();

    }

    protected void start() {
        logger.info("start consumer");
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
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

        MDC.remove("destination");
    }

    protected void process() {
        while (running) {
            try {
                MDC.put("destination", canalSourceConfig.getDestination());
                connector.connect();
                logger.info("process");
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(canalSourceConfig.getBatchSize());
                    // logger.info("message {}", message.toString());
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                    } else {
                        consume(new CanalRecord(message));
                    }

                    connector.ack(batchId);
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }


    static private class CanalRecord implements Record<Message> {

        private final Message record;


        public CanalRecord(Message message) {
            this.record = message;
        }

        @Override
        public Optional<String>  getKey() {
            return Optional.of(Long.toString(record.getId()));
        }

        @Override
        public Message getValue() {
            return record;
        }
    }

}
