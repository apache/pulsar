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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.testclient.utils.PaddingDecimalFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;

import io.netty.util.concurrent.DefaultThreadFactory;

public class PerformanceClient {

    static AtomicInteger msgSent = new AtomicInteger(0);
    private static final LongAdder messagesSent = new LongAdder();
    private static final LongAdder bytesSent = new LongAdder();
    private JCommander jc;

    static class Arguments {

        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = { "--conf-file" }, description = "Configuration file")
        public String confFile;

        @Parameter(names = { "-u", "--proxy-url" }, description = "Pulsar Proxy URL, e.g., \"ws://localhost:8080/\"", required = true)
        public String proxyURL;

        @Parameter(description = "persistent://tenant/ns/my-topic", required = true)
        public List<String> topics;

        @Parameter(names = { "-r", "--rate" }, description = "Publish rate msg/s across topics")
        public int msgRate = 100;

        @Parameter(names = { "-s", "--size" }, description = "Message size in byte")
        public int msgSize = 1024;

        @Parameter(names = { "-t", "--num-topic" }, description = "Number of topics")
        public int numTopics = 1;

        @Parameter(names = { "--auth_plugin" }, description = "Authentication plugin class name")
        public String authPluginClassName;

        @Parameter(
            names = { "--auth-params" },
            description = "Authentication parameters, whose format is determined by the implementation " +
                "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
                "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        public String authParams;

        @Parameter(names = { "-m",
                "--num-messages" }, description = "Number of messages to publish in total. If 0, it will keep publishing")
        public long numMessages = 0;

        @Parameter(names = { "-f", "--payload-file" }, description = "Use payload from a file instead of empty buffer")
        public String payloadFilename = null;

        @Parameter(names = { "-time",
                "--test-duration" }, description = "Test duration in secs. If 0, it will keep publishing")
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
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }

        if (arguments.topics.size() != 1) {
            System.err.println("Only one topic name is allowed");
            jc.usage();
            System.exit(-1);
        }

        if (arguments.confFile != null) {
            Properties prop = new Properties(System.getProperties());

            try {
                prop.load(new FileInputStream(arguments.confFile));
            } catch (IOException e) {
                log.error("Error in loading config file");
                jc.usage();
                System.exit(1);
            }

            if (arguments.proxyURL == null) {
                arguments.proxyURL = prop.getProperty("serviceUrl", "http://localhost:8080/");
            }

            if (arguments.authPluginClassName == null) {
                arguments.authPluginClassName = prop.getProperty("authPlugin", null);
            }

            if (arguments.authParams == null) {
                arguments.authParams = prop.getProperty("authParams", null);
            }
        }

        arguments.testTime = TimeUnit.SECONDS.toMillis(arguments.testTime);

        return arguments;

    }

    public void runPerformanceTest(long messages, long limit, int numOfTopic, int sizeOfMessage, String baseUrl,
            String topicName, String authPluginClassName, String authParams) throws InterruptedException, FileNotFoundException {
        ExecutorService executor = Executors.newCachedThreadPool(new DefaultThreadFactory("pulsar-perf-producer-exec"));
        HashMap<String, Tuple> producersMap = new HashMap<>();
        String produceBaseEndPoint = baseUrl + "ws/producer" + topicName;
        for (int i = 0; i < numOfTopic; i++) {
            String topic = numOfTopic > 1 ? produceBaseEndPoint + String.valueOf(i) : produceBaseEndPoint;
            URI produceUri = URI.create(topic);

            WebSocketClient produceClient = new WebSocketClient(new SslContextFactory(true));
            ClientUpgradeRequest produceRequest = new ClientUpgradeRequest();

            if (StringUtils.isNotBlank(authPluginClassName) && StringUtils.isNotBlank(authParams)) {
                try {
                    Authentication auth = AuthenticationFactory.create(authPluginClassName, authParams);
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
                RateLimiter rateLimiter = RateLimiter.create(limit);
                // Send messages on all topics/producers
                long totalSent = 0;
                while (true) {
                    for (String topic : producersMap.keySet()) {
                        if (messages > 0) {
                            if (totalSent >= messages) {
                                log.trace("------------------- DONE -----------------------");
                                Thread.sleep(10000);
                                System.exit(0);
                            }
                        }

                        rateLimiter.acquire();

                        if (producersMap.get(topic).getSocket().getSession() == null) {
                            Thread.sleep(10000);
                            System.exit(0);
                        }
                        producersMap.get(topic).getSocket().sendMsg(String.valueOf(totalSent++), sizeOfMessage);
                        messagesSent.increment();
                        bytesSent.add(sizeOfMessage);
                    }
                }

            } catch (Throwable t) {
                log.error(t.getMessage());
                System.exit(0);
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

            double rate = messagesSent.sumThenReset() / elapsed;
            double throughput = bytesSent.sumThenReset() / elapsed / 1024 / 1024 * 8;

            reportHistogram = SimpleTestProducerSocket.recorder.getIntervalHistogram(reportHistogram);

            log.info(
                    "Throughput produced: {}  msg/s --- {} Mbit/s --- Latency: mean: {} ms - med: {} ms - 95pct: {} ms - 99pct: {} ms - 99.9pct: {} ms - 99.99pct: {} ms",
                    throughputFormat.format(rate), throughputFormat.format(throughput),
                    dec.format(reportHistogram.getMean() / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(50) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(95) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(reportHistogram.getValueAtPercentile(99.99) / 1000.0));

            histogramLogWriter.outputIntervalHistogram(reportHistogram);
            reportHistogram.reset();

            oldTime = now;
        }

        TimeUnit.SECONDS.sleep(100);

        executor.shutdown();

    }

    public static void main(String[] args) throws Exception {
        PerformanceClient test = new PerformanceClient();
        Arguments arguments = test.loadArguments(args);
        test.runPerformanceTest(arguments.numMessages, arguments.msgRate, arguments.numTopics, arguments.msgSize,
                arguments.proxyURL, arguments.topics.get(0), arguments.authPluginClassName, arguments.authParams);
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

    static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 8);
    static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 7);
    private static final Logger log = LoggerFactory.getLogger(PerformanceClient.class);

}
