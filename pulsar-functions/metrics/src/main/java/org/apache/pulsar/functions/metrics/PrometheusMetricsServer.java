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

package org.apache.pulsar.functions.metrics;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.metrics.sink.AbstractWebSink;
import org.apache.pulsar.functions.metrics.sink.PrometheusSink;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceControlGrpc;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class PrometheusMetricsServer {
    @Parameter(names = "--function_details", description = "Function details json\n", required = true)
    protected String functionDetailsJsonString;

    @Parameter(names = "--prometheus_port", description = "Port to listen for prometheus requests\n", required = true)
    protected int prometheusPort;

    @Parameter(names = "--grpc_port", description = "GRPC Port to query the metrics from instance\n", required = true)
    protected int grpc_port;

    @Parameter(names = "--collection_interval", description = "Number in seconds between collection interval\n", required = true)
    protected int metricsCollectionInterval;

    private FunctionDetails functionDetails;
    private MetricsSink metricsSink;
    private ManagedChannel channel;
    private InstanceControlGrpc.InstanceControlFutureStub stub;
    private ScheduledExecutorService timer;

    public PrometheusMetricsServer() { }


    public void start() throws Exception {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (functionDetailsJsonString.charAt(0) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(1);
        }
        if (functionDetailsJsonString.charAt(functionDetailsJsonString.length() - 1) == '\'') {
            functionDetailsJsonString = functionDetailsJsonString.substring(0, functionDetailsJsonString.length() - 1);
        }
        JsonFormat.parser().merge(functionDetailsJsonString, functionDetailsBuilder);
        functionDetails = functionDetailsBuilder.build();

        metricsSink = new PrometheusSink();
        Map<String, String> config = new HashMap<>();
        config.put(AbstractWebSink.KEY_PATH, "/metrics");
        config.put(AbstractWebSink.KEY_PORT, String.valueOf(prometheusPort));
        metricsSink.init(config);

        channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpc_port)
                .usePlaintext(true)
                .build();
        stub = InstanceControlGrpc.newFutureStub(channel);

        if (metricsCollectionInterval > 0) {
            timer = Executors.newSingleThreadScheduledExecutor();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    CompletableFuture<InstanceCommunication.MetricsData> result = getMetrics();
                    try {
                        metricsSink.processRecord(result.get(), functionDetails);
                    } catch (Exception e) {
                        log.error("Getting metrics data failed {}/{}/{}",
                                functionDetails.getTenant(),
                                functionDetails.getNamespace(),
                                functionDetails.getName(),
                                e);
                    }
                }
            }, metricsCollectionInterval, metricsCollectionInterval, TimeUnit.SECONDS);
        }
    }

    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics() {
        CompletableFuture<InstanceCommunication.MetricsData> retval = new CompletableFuture<>();
        ListenableFuture<InstanceCommunication.MetricsData> response = stub.withDeadlineAfter(10, TimeUnit.SECONDS).getAndResetMetrics(Empty.newBuilder().build());
        Futures.addCallback(response, new FutureCallback<InstanceCommunication.MetricsData>() {
            @Override
            public void onFailure(Throwable throwable) {
                retval.completeExceptionally(throwable);
            }

            @Override
            public void onSuccess(InstanceCommunication.MetricsData t) {
                retval.complete(t);
            }
        });
        return retval;
    }

    public static void main(String[] args) throws Exception {
        PrometheusMetricsServer server = new PrometheusMetricsServer();
        JCommander jcommander = new JCommander(server);
        jcommander.setProgramName("PrometheusMetricsServer");

        // parse args by JCommander
        jcommander.parse(args);
        server.start();
    }
}
