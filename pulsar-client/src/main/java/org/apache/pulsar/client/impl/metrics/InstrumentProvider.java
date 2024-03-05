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

package org.apache.pulsar.client.impl.metrics;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import java.util.Optional;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

public class InstrumentProvider {

    private final Meter meter;

    public InstrumentProvider(ClientConfigurationData conf) {
        OpenTelemetry otel = conf.getOpenTelemetry();
        if (otel == null) {
            // By default, metrics are disabled, unless the OTel java agent is configured.
            // This allows to enable metrics without any code change.
            otel = GlobalOpenTelemetry.get();
        }

        this.meter = otel.getMeterProvider()
                .meterBuilder("org.apache.pulsar.client")
                .setInstrumentationVersion(PulsarVersion.getVersion())
                .build();
    }

    public Counter newCounter(String name, Unit unit, String description, String topic, Attributes attributes) {
        return new Counter(meter, name, unit, description, topic, attributes);
    }

    public UpDownCounter newUpDownCounter(String name, Unit unit, String description, String topic, Attributes attributes) {
        return new UpDownCounter(meter, name, unit, description, topic, attributes);
    }

    public LatencyHistogram newLatencyHistogram(String name, String description, String topic, Attributes attributes) {
        return new LatencyHistogram(meter, name, description, topic, attributes);
    }
}
