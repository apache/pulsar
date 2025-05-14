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
package org.apache.pulsar.broker.stats;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder;
import java.io.Closeable;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.opentelemetry.Constants;
import org.apache.pulsar.opentelemetry.OpenTelemetryService;

public class PulsarBrokerOpenTelemetry implements Closeable {

    public static final String SERVICE_NAME = "pulsar-broker";

    @Getter
    private final OpenTelemetryService openTelemetryService;

    @Getter
    private final Meter meter;

    public PulsarBrokerOpenTelemetry(ServiceConfiguration config,
                                 @VisibleForTesting Consumer<AutoConfiguredOpenTelemetrySdkBuilder> builderCustomizer) {
        openTelemetryService = OpenTelemetryService.builder()
                .clusterName(config.getClusterName())
                .serviceName(SERVICE_NAME)
                .serviceVersion(PulsarVersion.getVersion())
                .builderCustomizer(builderCustomizer)
                .build();
        meter = openTelemetryService.getOpenTelemetry().getMeter(Constants.BROKER_INSTRUMENTATION_SCOPE_NAME);
    }

    public OpenTelemetry getOpenTelemetry() {
        return openTelemetryService.getOpenTelemetry();
    }

    @Override
    public void close() {
        openTelemetryService.close();
    }
}
