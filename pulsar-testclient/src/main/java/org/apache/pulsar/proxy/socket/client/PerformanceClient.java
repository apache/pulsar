/*
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
package org.apache.pulsar.proxy.socket.client;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.testclient.CmdBase;
import org.apache.pulsar.testclient.IMessageFormatter;
import org.apache.pulsar.testclient.PerfClientUtils;
import org.apache.pulsar.testclient.PositiveNumberParameterConvert;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(name = "websocket-producer", description = "Test pulsar websocket producer performance.")
public class PerformanceClient extends CmdBase {

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();
    private static IMessageFormatter messageFormatter = null;

    @Option(names = { "-cf", "--conf-file" }, description = "Configuration file")
    public String confFile;

    @Option(names = { "-u", "--proxy-url" }, description = "Pulsar Proxy URL, e.g., \"ws://localhost:8080/\"")
    public String proxyURL;

    @Parameters(description = "persistent://tenant/ns/my-topic", arity = "1")
    public List<String> topics;

    @Option(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
    public int msgRate = 100;

    @Option(names = { "-s", "--size" }, description = "Message size in byte")
    public int msgSize = 1024;

    @Option(names = { "-t", "--num-topic" }, description = "Number of topics",
            converter = PositiveNumberParameterConvert.class
    )
    public int numTopics = 1;

    @Option(names = { "--auth_plugin" }, description = "Authentication plugin class name", hidden = true)
    public String deprecatedAuthPluginClassName;

    @Option(names = { "--auth-plugin" }, description = "Authentication plugin class name")
    public String authPluginClassName;

    @Option(
        names = { "--auth-params" },
        description = "Authentication parameters, whose format is determined by the implementation "
                + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".")
    public String authParams;

    @Option(names = { "-m",
            "--num-messages" }, description = "Number of messages to publish in total. If <= 0, it will keep"
            + " publishing")
    public long numMessages = 0;

    @Option(names = { "-f", "--payload-file" }, description = "Use payload from a file instead of empty buffer")
    public String payloadFilename = null;

    @Option(names = { "-e", "--payload-delimiter" },
            description = "The delimiter used to split lines when using payload from a file")
    // here escaping \n since default value will be printed with the help text
    public String payloadDelimiter = "\\n";

    @Option(names = { "-fp", "--format-payload" },
            description = "Format %%i as a message index in the stream from producer and/or %%t as the timestamp"
                    + " nanoseconds")
    public boolean formatPayload = false;

    @Option(names = {"-fc", "--format-class"}, description = "Custom Formatter class name")
    public String formatterClass = "org.apache.pulsar.testclient.DefaultMessageFormatter";

    @Option(names = { "-time",
            "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
    public long testTime = 0;

    public PerformanceClient() {
        super("websocket-producer");
    }


    @Spec
    CommandSpec spec;

    public void loadArguments() {
        CommandLine commander = spec.commandLine();

        if (isBlank(this.authPluginClassName) && !isBlank(this.deprecatedAuthPluginClassName)) {
            this.authPluginClassName = this.deprecatedAuthPluginClassName;
        }

        if (this.topics.size() != 1) {
            System.err.println("Only one topic name is allowed");
            commander.usage(commander.getOut());
            PerfClientUtils.exit(1);
        }

        if (this.confFile != null) {
            Properties prop = new Properties(System.getProperties());

            try {
                prop.load(new FileInputStream(this.confFile));
            } catch (IOException e) {
                log.error("Error in loading config file");
                commander.usage(commander.getOut());
                PerfClientUtils.exit(1);
            }

            if (isBlank(this.proxyURL)) {
                String webSocketServiceUrl = prop.getProperty("webSocketServiceUrl");
                if (isNotBlank(webSocketServiceUrl)) {
                    this.proxyURL = webSocketServiceUrl;
                } else {
                    String webServiceUrl = isNotBlank(prop.getProperty("webServiceUrl"))
                            ? prop.getProperty("webServiceUrl")
                            : prop.getProperty("serviceUrl");
                    if (isNotBlank(webServiceUrl)) {
                        if (webServiceUrl.startsWith("ws://") || webServiceUrl.startsWith("wss://")) {
                            this.proxyURL = webServiceUrl;
                        } else if (webServiceUrl.startsWith("http://") || webServiceUrl.startsWith("https://")) {
                            this.proxyURL = webServiceUrl.replaceFirst("^http", "ws");
                        }
                    }
                }
            }

            if (this.authPluginClassName == null) {
                this.authPluginClassName = prop.getProperty("authPlugin", null);
            }

            if (this.authParams == null) {
                this.authParams = prop.getProperty("authParams", null);
            }
        }

        if (isBlank(this.proxyURL)) {
            this.proxyURL = "ws://localhost:8080/";
        }

        if (!this.proxyURL.endsWith("/")) {
            this.proxyURL += "/";
        }

    }

    public void runPerformanceTest() throws InterruptedException, IOException {
        // Read payload data from file if needed
        final byte[] payloadBytes = new byte[this.msgSize];
        Random random = new Random(0);
        List<byte[]> payloadByteList = new ArrayList<>();
        if (this.payloadFilename != null) {
            Path payloadFilePath = Paths.get(this.payloadFilename);
            if (Files.notExists(payloadFilePath) || Files.size(payloadFilePath) == 0)  {
                throw new IllegalArgumentException("Payload file doesn't exist or it is empty.");
            }
            // here escaping the default payload delimiter to correct value
            String delimiter = this.payloadDelimiter.equals("\\n") ? "\n" : this.payloadDelimiter;
            String[] payloadList = new String(Files.readAllBytes(payloadFilePath), StandardCharsets.UTF_8)
                    .split(delimiter);
            log.info("Reading payloads from {} and {} records read", payloadFilePath.toAbsolutePath(),
                    payloadList.length);
            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }

            if (this.formatPayload) {
                messageFormatter = getMessageFormatter(this.formatterClass);
            }
        } else {
            for (int i = 0; i < payloadBytes.length; ++i) {
                // The value of the payloadBytes is from A to Z and the ASCII of A-Z is 65-90, so it needs to add 65
                payloadBytes[i] = (byte) (random.nextInt(26) + 65);
            }
        }

        ExecutorService executor = Executors.newCachedThreadPool(
                new DefaultThreadFactory("pulsar-perf-producer-exec"));
        HashMap<String, Tuple> producersMap = new HashMap<>();
        String topicName = this.topics.get(0);
        String restPath = TopicName.get(topicName).getRestPath();
        String produceBaseEndPoint = TopicName.get(topicName).isV2()
                ? this.proxyURL + "ws/v2/producer/" + restPath : this.proxyURL + "ws/producer/" + restPath;
        for (int i = 0; i < this.numTopics; i++) {
            String topic = this.numTopics > 1 ? produceBaseEndPoint + i : produceBaseEndPoint;
            URI produceUri = URI.create(topic);

            WebSocketClient produceClient = new WebSocketClient(new SslContextFactory(true));
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();

            if (StringUtils.isNotBlank(this.authPluginClassName) && StringUtils.isNotBlank(this.authParams)) {
                try {
                    Authentication auth = AuthenticationFactory.create(this.authPluginClassName,
                            this.authParams);
                    auth.start();
                    AuthenticationDataProvider authData = auth.getAuthData();
                    if (authData.hasDataForHttp()) {
                        for (Map.Entry<String, String> kv : authData.getHttpHeaders()) {
                            produceRequest.setHeader(kv.getKey(), kv.getValue());
                        }
                    }
                } catch (Exception e) {
                    log.error("Authentication plugin error: " + e.getMessage());
                }
            }

            SimpleTestProducerSocket produceSocket = new SimpleTestProducerSocket();

            try {
                produceClient.start();
                produceClient.connect(produceSocket, produceUri, produceRequest);
            } catch (IOException e1) {
                log.error("Fail in connecting: [{}]", e1.getMessage());
                return;
            } catch (Exception e1) {
                log.error("Fail in starting client[{}]", e1.getMessage());
                return;
            }

            producersMap.put(produceUri.toString(), new Tuple(produceClient, produceRequest, produceSocket));
        }

        // connection to be established
        TimeUnit.SECONDS.sleep(5);

        executor.submit(() -> {
            try {
                RateLimiter rateLimiter = RateLimiter.create(this.msgRate);
                long startTime = System.nanoTime();
                long testEndTime = startTime + (long) (this.testTime * 1e9);
                // Send messages on all topics/producers
                long totalSent = 0;
                while (true) {
                    for (String topic : producersMap.keySet()) {
                        if (this.testTime > 0 && System.nanoTime() > testEndTime) {
                            log.info("------------- DONE (reached the maximum duration: [{} seconds] of production) "
                                    + "--------------", this.testTime);
                            PerfClientUtils.exit(0);
                        }

                        if (this.numMessages > 0) {
                            if (totalSent >= this.numMessages) {
                                log.trace("------------- DONE (reached the maximum number: [{}] of production) "
                                        + "--------------", this.numMessages);
                                Thread.sleep(10000);
                                PerfClientUtils.exit(0);
                            }
                        }

                        rateLimiter.acquire();

                        if (producersMap.get(topic).getSocket().getSession() == null) {
                            Thread.sleep(10000);
                            PerfClientUtils.exit(0);
                        }

                        byte[] payloadData;
                        if (this.payloadFilename != null) {
                            if (messageFormatter != null) {
                                payloadData = messageFormatter.formatMessage("", totalSent,
                                        payloadByteList.get(random.nextInt(payloadByteList.size())));
                            } else {
                                payloadData = payloadByteList.get(random.nextInt(payloadByteList.size()));
                            }
                        } else {
                            payloadData = payloadBytes;
                        }
                        producersMap.get(topic).getSocket().sendMsg(String.valueOf(totalSent++), payloadData);
                        messagesSent.increment();
                        bytesSent.add(payloadData.length);
                        totalMessagesSent.increment();
                        totalBytesSent.add(payloadData.length);
                    }
                }

            } catch (Throwable t) {
                log.error(t.getMessage());
                PerfClientUtils.exit(0);
            }
        });

        // Print report stats
        long oldTime = System.nanoTime();

        Histogram reportHistogram = null;

        String statsFileName = "perf-websocket-producer-" + System.currentTimeMillis() + ".hgrm";
        log.info("Dumping latency stats to {} \n", statsFileName);

        PrintStream histogramLog = new PrintStream(new FileOutputStream(statsFileName), false);
        HistogramLogWriter histogramLogWriter = new HistogramLogWriter(histogramLog);

        // Some log header bits
        histogramLogWriter.outputLogFormatVersion();
        histogramLogWriter.outputLegend();

        while (true) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }

            long now = System.nanoTime();
            double elapsed = (now - oldTime) / 1e9;

            long total = totalMessagesSent.sum();
            double rate = messagesSent.sumThenReset() / elapsed;
            double throughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024 * 8;

            reportHistogram = SimpleTestProducerSocket.recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput produced: {} msg --- {}  msg/s --- {} Mbit/s --- Latency: mean: {} ms - med: {} ms "
                            + "- 95pct: {} ms - 99pct: {} ms - 99.9pct: {} ms - 99.99pct: {} ms",
                    INTFORMAT.format(total),
                    THROUGHPUTFORMAT.format(rate),
                    THROUGHPUTFORMAT.format(throughput),
                    DEC.format(reportHistogram.getMean() / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    DEC.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0));

            histogramLogWriter.outputIntervalHistogram(reportHistogram);
            reportHistogram.reset();

            oldTime = now;
        }

        TimeUnit.SECONDS.sleep(100);

        executor.shutdown();

    }

    static IMessageFormatter getMessageFormatter(String formatterClass) {
        try {
            ClassLoader classLoader = PerformanceClient.class.getClassLoader();
            Class clz = classLoader.loadClass(formatterClass);
            return (IMessageFormatter) clz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void run() throws Exception {
        loadArguments();
        PerfClientUtils.printJVMInformation(log);
        long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));
        runPerformanceTest();
    }

    private class Tuple {
        private WebSocketClient produceClient;
        private ClientUpgradeRequest produceRequest;
        private SimpleTestProducerSocket produceSocket;

        public Tuple(WebSocketClient produceClient, ClientUpgradeRequest produceRequest,
                SimpleTestProducerSocket produceSocket) {
            this.produceClient = produceClient;
            this.produceRequest = produceRequest;
            this.produceSocket = produceSocket;
        }

        public SimpleTestProducerSocket getSocket() {
            return produceSocket;
        }

    }

    private static void printAggregatedThroughput(long start) {
        double elapsed = (System.nanoTime() - start) / 1e9;
        double rate = totalMessagesSent.sum() / elapsed;
        double throughput = totalBytesSent.sum() / elapsed / 1024 / 1024 * 8;
        log.info(
                "Aggregated throughput stats --- {} records sent --- {} msg/s --- {} Mbit/s",
                totalMessagesSent,
                TOTALFORMAT.format(rate),
                TOTALFORMAT.format(throughput));
    }

    private static void printAggregatedStats() {
        Histogram reportHistogram = SimpleTestProducerSocket.recorder.getIntervalHistogram();

        log.info(
                "Aggregated latency stats --- Latency: mean: {} ms - med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} "
                        + "- 99.99pct: {} - 99.999pct: {} - Max: {}",
                DEC.format(reportHistogram.getMean()), reportHistogram.getValueAtPercentile(50),
                reportHistogram.getValueAtPercentile(95), reportHistogram.getValueAtPercentile(99),
                reportHistogram.getValueAtPercentile(99.9), reportHistogram.getValueAtPercentile(99.99),
                reportHistogram.getValueAtPercentile(99.999), reportHistogram.getMaxValue());
    }

    static final DecimalFormat THROUGHPUTFORMAT = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat DEC = new PaddingDecimalFormat("0.000", 7);
    static final DecimalFormat TOTALFORMAT = new DecimalFormat("0.000");
    static final DecimalFormat INTFORMAT = new PaddingDecimalFormat("0", 7);
    private static final Logger log = LoggerFactory.getLogger(PerformanceClient.class);

}
