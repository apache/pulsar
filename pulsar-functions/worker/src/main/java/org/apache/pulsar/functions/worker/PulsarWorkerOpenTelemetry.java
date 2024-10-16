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
package org.apache.pulsar.functions.worker;

import io.opentelemetry.api.metrics.Meter;
import java.io.Closeable;
import lombok.Getter;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.opentelemetry.OpenTelemetryService;

public class PulsarWorkerOpenTelemetry implements Closeable {

    public static final String SERVICE_NAME = "pulsar-function-worker";
    public static final String INSTRUMENTATION_SCOPE_NAME = "org.apache.pulsar.function_worker";

    private final OpenTelemetryService openTelemetryService;

    @Getter
    private final Meter meter;

    public PulsarWorkerOpenTelemetry(WorkerConfig workerConfig) {
        openTelemetryService = OpenTelemetryService.builder()
                .clusterName(workerConfig.getPulsarFunctionsCluster())
                .serviceName(SERVICE_NAME)
                .serviceVersion(PulsarVersion.getVersion())
                .build();
        meter = openTelemetryService.getOpenTelemetry().getMeter(INSTRUMENTATION_SCOPE_NAME);
    }

    @Override
    public void close() {
        openTelemetryService.close();
    }
}
