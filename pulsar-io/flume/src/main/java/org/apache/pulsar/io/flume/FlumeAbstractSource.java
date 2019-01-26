package org.apache.pulsar.io.flume;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.cli.ParseException;
import org.apache.flume.*;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.util.SSLUtil;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.flume.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public abstract class FlumeAbstractSource<V> extends PushSource<V> {

    private static final Logger log = LoggerFactory
            .getLogger(FlumeAbstractSource.class);

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

        SSLUtil.initGlobalSSLParameters();
        FlumeConfig flumeConfig = FlumeConfig.load(config);
        String agentName = flumeConfig.getName();
        boolean reload = !flumeConfig.getNoReloadConf();
        boolean isZkConfigured = false;
        if (flumeConfig.getZkConnString().length() > 0) {
            isZkConfigured = true;
        }
        Application application;
        if (isZkConfigured) {
            // get options
            String zkConnectionStr = flumeConfig.getZkConnString();
            String baseZkPath = flumeConfig.getZkBasePath();
            if (reload) {
                EventBus eventBus = new EventBus(agentName + "-event-bus");
                List<LifecycleAware> components = Lists.newArrayList();
                PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                        new PollingZooKeeperConfigurationProvider(
                                agentName, zkConnectionStr, baseZkPath, eventBus);
                components.add(zookeeperConfigurationProvider);
                application = new Application(components);
                eventBus.register(application);
            } else {
                StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                        new StaticZooKeeperConfigurationProvider(
                                agentName, zkConnectionStr, baseZkPath);
                application = new Application();
                application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
            }

        } else {
            File configurationFile = new File(flumeConfig.getConfFile());
             /*
         * The following is to ensure that by default the agent will fail on
         * startup if the file does not exist.
         */
            if (!configurationFile.exists()) {
                // If command line invocation, then need to fail fast
                if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) ==
                        null) {
                    String path = configurationFile.getPath();
                    try {
                        path = configurationFile.getCanonicalPath();
                    } catch (IOException ex) {
                        log.error("Failed to read canonical path for file: " + path,
                                ex);
                    }
                    throw new ParseException(
                            "The specified configuration file does not exist: " + path);
                }
            }
            List<LifecycleAware> components = Lists.newArrayList();

            if (reload) {
                EventBus eventBus = new EventBus(agentName + "-event-bus");
                PollingPropertiesFileConfigurationProvider configurationProvider =
                        new PollingPropertiesFileConfigurationProvider(
                                agentName, configurationFile, eventBus, 30);
                components.add(configurationProvider);
                application = new Application(components);
                eventBus.register(application);
            } else {
                PropertiesFileConfigurationProvider configurationProvider =
                        new PropertiesFileConfigurationProvider(agentName, configurationFile);
                application = new Application();
                application.handleConfigurationEvent(configurationProvider.getConfiguration());
            }
        }
        application.start();
        this.start();

        final Application appReference = application;
        Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
            @Override
            public void run() {
                appReference.stop();
            }
        });
    }

    public abstract V extractValue(String message);

    protected void start() {
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

        thread.setName("canal source thread");
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        log.info("close canal source");
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
                    BlockingQueue<Map<String, Object>> blockingQueue = SinkOfFlume.getQueue();
                    while (!blockingQueue.isEmpty()) {
                        Map<String, Object> message = blockingQueue.take();
                        System.out.println(message);
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
