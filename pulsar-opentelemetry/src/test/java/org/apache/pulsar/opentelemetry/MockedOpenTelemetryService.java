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
package org.apache.pulsar.opentelemetry;

import com.google.protobuf.Message;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.pulsar.common.util.PortManager;

public class MockedOpenTelemetryService {
    private static final String MOCKED_SERVICE_NAME = "mocked-service-name";
    private static final String EXPORT_METHOD_NAME = "export";
    private final int port;
    private final Server server;
    private final MetricsDataStore metricsDataStore;

    public MockedOpenTelemetryService() {
        this(PortManager.nextLockedFreePort());
    }

    public MockedOpenTelemetryService(int port) {
        this.metricsDataStore = new MetricsDataStore();
        this.port = port;
        this.server = ServerBuilder.forPort(port).addService(getServiceDefinition()).build();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
        PortManager.releaseLockedPort(port);
    }

    public MetricsDataStore getMetricsDataStore() {
        return metricsDataStore;
    }

    public int getPort() {
        return port;
    }

    private ServerServiceDefinition getServiceDefinition() {
        MethodDescriptor<ExportMetricsServiceRequest, ExportMetricsServiceResponse> exportMethodDescriptor =
                MethodDescriptor.<ExportMetricsServiceRequest, ExportMetricsServiceResponse>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(
                                MethodDescriptor.generateFullMethodName(MOCKED_SERVICE_NAME, EXPORT_METHOD_NAME))
                        .setRequestMarshaller(new ProtoMarshaller<>(ExportMetricsServiceRequest.getDefaultInstance()))
                        .setResponseMarshaller(new ProtoMarshaller<>(ExportMetricsServiceResponse.getDefaultInstance()))
                        .build();
        ServerCalls.UnaryMethod<ExportMetricsServiceRequest, ExportMetricsServiceResponse> exportMethodHandler =
                (request, responseObserver) -> {
                    processMetrics(request);
                    ExportMetricsServiceResponse response = ExportMetricsServiceResponse.newBuilder()
                            .setPartialSuccess(ExportMetricsServiceResponse.getDefaultInstance().getPartialSuccess())
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                };
        return ServerServiceDefinition.builder(MOCKED_SERVICE_NAME)
                .addMethod(exportMethodDescriptor, ServerCalls.asyncUnaryCall(exportMethodHandler)).build();
    }

    private int processMetrics(ExportMetricsServiceRequest request) {
        int count = 0;
        for (ResourceMetrics resourceMetrics : request.getResourceMetricsList()) {
            metricsDataStore.addMetrics(resourceMetrics, null, null);
            count++;
        }
        return count;
    }

    private static class ProtoMarshaller<T extends Message> implements MethodDescriptor.Marshaller<T> {
        private final T defaultInstance;

        public ProtoMarshaller(T defaultInstance) {
            this.defaultInstance = defaultInstance;
        }

        @Override
        public InputStream stream(T t) {
            return defaultInstance.toByteString().newInput();
        }

        @Override
        public T parse(InputStream inputStream) {
            try {
                return (T) defaultInstance.getParserForType().parseFrom(inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class MetricsDataStore {
        private final ConcurrentMap<String, List<StoredMetric>> metricsData = new ConcurrentHashMap<>();
        private final AtomicInteger idGenerator = new AtomicInteger(0);

        public void addMetrics(ResourceMetrics resourceMetrics, ScopeMetrics scopeMetrics, Metric metric) {
            String metricName = metric.getName();
            metricsData.computeIfAbsent(metricName, k -> Collections.synchronizedList(new ArrayList<>()))
                    .add(new StoredMetric(
                            idGenerator.incrementAndGet(),
                            System.currentTimeMillis(),
                            resourceMetrics,
                            scopeMetrics,
                            metric
                    ));
        }

        public List<StoredMetric> getMetrics(String metricName) {
            return metricsData.getOrDefault(metricName, Collections.emptyList());
        }

        public List<StoredMetric> getAllMetric() {
            return metricsData.values().stream().flatMap(List::stream).toList();
        }

        public int getMetricCount() {
            return metricsData.values().stream().mapToInt(List::size).sum();
        }
    }

    @Getter
    public static class StoredMetric {
        private final int id;
        private final long timestamp;
        private final ResourceMetrics resourceMetrics;
        private final ScopeMetrics scopeMetrics;
        private final Metric metric;

        public StoredMetric(int id, long timestamp, ResourceMetrics resourceMetrics, ScopeMetrics scopeMetrics,
                            Metric metric) {
            this.id = id;
            this.timestamp = timestamp;
            this.resourceMetrics = resourceMetrics;
            this.scopeMetrics = scopeMetrics;
            this.metric = metric;
        }
    }
}
