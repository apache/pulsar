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
package org.apache.pulsar.proxy.socket.client;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
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
import org.apache.pulsar.testclient.IMessageFormatter;
import org.apache.pulsar.testclient.PerfClientUtils;
import org.apache.pulsar.testclient.PositiveNumberParameterValidator;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceClient {

    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private static final LongAdder totalMessagesSent = new LongAdder();
    private static final LongAdder totalBytesSent = new LongAdder();
    private static IMessageFormatter messageFormatter = null;
    private JCommander jc;

    @Parameters(commandDescription = "Test pulsar websocket producer performance.")
    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "-cf", "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(names = { "-u", "--proxy-url" }, description = "Pulsar Proxy URL, e.g., \"ws://localhost:8080/\"")
        public String proxyURL;

        @Parameter(description = "persistent://tenant/ns/my-topic", required = true)
        public List<String> topics;

        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 100;

        @Parameter(names = { "-s", "--size" }, description = "Message size in byte")
        public int msgSize = 1024;

        @Parameter(names = { "-t", "--num-topic" }, description = "Number of topics",
                validateWith = PositiveNumberParameterValidator.class)
        public int numTopics = 1;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name", hidden = true)
        public String deprecatedAuthPluginClassName;

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation "
                    + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                    + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        public String authParams;

        @Parameter(names = { "-m",
                "--num-messages" }, description = "Number of messages to publish in total. If <= 0, it will keep"
                + " publishing")
        public long numMessages = 0;

        @Parameter(names = { "-f", "--payload-file" }, description = "Use payload from a file instead of empty buffer")
        public String payloadFilename = null;

        @Parameter(names = { "-e", "--payload-delimiter" },
                description = "The delimiter used to split lines when using payload from a file")
        // here escaping \n since default value will be printed with the help text
        public String payloadDelimiter = "\\n";

        @Parameter(names = { "-fp", "--format-payload" },
                description = "Format %i as a message index in the stream from producer and/or %t as the timestamp"
                        + " nanoseconds")
        public boolean formatPayload = false;

        @Parameter(names = {"-fc", "--format-class"}, description = "Custom Formatter class name")
        public String formatterClass = "org.apache.pulsar.testclient.DefaultMessageFormatter";

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If <= 0, it will keep publishing")
        public long testTime = 0;
    }

    public Arguments loadArguments(String[] args) {
        Arguments arguments = new Arguments();
        jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf websocket-producer");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (isBlank(arguments.authPluginClassName) && !isBlank(arguments.deprecatedAuthPluginClassName)) {
            arguments.authPluginClassName = arguments.deprecatedAuthPluginClassName;
        }

        if (arguments.topics.size() != 1) {
            System.err.println("Only one topic name is allowed");
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.confFile != null) {
            Properties prop = new Properties(System.getProperties());

            try {
                prop.load(new FileInputStream(arguments.confFile));
            } catch (IOException e) {
                log.error("Error in loading config file");
                jc.usage();
                PerfClientUtils.exit(1);
            }

            if (isBlank(arguments.proxyURL)) {
                String webSocketServiceUrl = prop.getProperty("webSocketServiceUrl");
                if (isNotBlank(webSocketServiceUrl)) {
                    arguments.proxyURL = webSocketServiceUrl;
                } else {
                    String webServiceUrl = isNotBlank(prop.getProperty("webServiceUrl"))
                            ? prop.getProperty("webServiceUrl")
                            : prop.getProperty("serviceUrl");
                    if (isNotBlank(webServiceUrl)) {
                        if (webServiceUrl.startsWith("ws://") || webServiceUrl.startsWith("wss://")) {
                            arguments.proxyURL = webServiceUrl;
                        } else if (webServiceUrl.startsWith("http://") || webServiceUrl.startsWith("https://")) {
                            arguments.proxyURL = webServiceUrl.replaceFirst("^http", "ws");
                        }
                    }
                }
            }

            if (arguments.authPluginClassName == null) {
                arguments.authPluginClassName = prop.getProperty("authPlugin", null);
            }

            if (arguments.authParams == null) {
                arguments.authParams = prop.getProperty("authParams", null);
            }
        }

        if (isBlank(arguments.proxyURL)) {
            arguments.proxyURL = "ws://localhost:8080/";
        }

        if (!arguments.proxyURL.endsWith("/")) {
            arguments.proxyURL += "/";
        }

        return arguments;

    }

    public void runPerformanceTest(Arguments arguments) throws InterruptedException, IOException {
        // Read payload data from file if needed
        final byte[] payloadBytes = new byte[arguments.msgSize];
        Random random = new Random(0);
        List<byte[]> payloadByteList = new ArrayList<>();
        if (arguments.payloadFilename != null) {
            Path payloadFilePath = Paths.get(arguments.payloadFilename);
            if (Files.notExists(payloadFilePath) || Files.size(payloadFilePath) == 0)  {
                throw new IllegalArgumentException("Payload file doesn't exist or it is empty.");
            }
            // here escaping the default payload delimiter to correct value
            String delimiter = arguments.payloadDelimiter.equals("\\n") ? "\n" : arguments.payloadDelimiter;
            String[] payloadList = new String(Files.readAllBytes(payloadFilePath), StandardCharsets.UTF_8)
                    .split(delimiter);
            log.info("Reading payloads from {} and {} records read", payloadFilePath.toAbsolutePath(),
                    payloadList.length);
            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }

            if (arguments.formatPayload) {
                messageFormatter = getMessageFormatter(arguments.formatterClass);
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
        String topicName = arguments.topics.get(0);
        String restPath = TopicName.get(topicName).getRestPath();
        String produceBaseEndPoint = TopicName.get(topicName).isV2()
                ? arguments.proxyURL + "ws/v2/producer/" + restPath : arguments.proxyURL + "ws/producer/" + restPath;
        for (int i = 0; i < arguments.numTopics; i++) {
            String topic = arguments.numTopics > 1 ? produceBaseEndPoint + i : produceBaseEndPoint;
            URI produceUri = URI.create(topic);

            WebSocketClient produceClient = new WebSocketClient(new SslContextFactory(true));
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();

            if (StringUtils.isNotBlank(arguments.authPluginClassName) && StringUtils.isNotBlank(arguments.authParams)) {
                try {
                    Authentication auth = AuthenticationFactory.create(arguments.authPluginClassName,
                            arguments.authParams);
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
                RateLimiter rateLimiter = RateLimiter.create(arguments.msgRate);
                long startTime = System.nanoTime();
                long testEndTime = startTime + (long) (arguments.testTime * 1e9);
                // Send messages on all topics/producers
                long totalSent = 0;
                while (true) {
                    for (String topic : producersMap.keySet()) {
                        if (arguments.testTime > 0 && System.nanoTime() > testEndTime) {
                            log.info("------------- DONE (reached the maximum duration: [{} seconds] of production) "
                                    + "--------------", arguments.testTime);
                            PerfClientUtils.exit(0);
                        }

                        if (arguments.numMessages > 0) {
                            if (totalSent >= arguments.numMessages) {
                                log.trace("------------- DONE (reached the maximum number: [{}] of production) "
                                        + "--------------", arguments.numMessages);
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
                        if (arguments.payloadFilename != null) {
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

    public static void main(String[] args) throws Exception {
        PerformanceClient test = new PerformanceClient();
        Arguments arguments = test.loadArguments(args);
        PerfClientUtils.printJVMInformation(log);
        long start = System.nanoTime();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printAggregatedThroughput(start);
            printAggregatedStats();
        }));
        test.runPerformanceTest(arguments);
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
