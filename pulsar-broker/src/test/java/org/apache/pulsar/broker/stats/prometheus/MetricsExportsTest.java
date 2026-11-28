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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetricsExportsTest {

    private static final String VERSION_INFO = "pulsar_version_info";

    @Test
    public void testRegisterPutsTheNettyDirectMemoryGaugesAndTheVersionOnTheGivenRegistry() {
        PrometheusRegistry registry = new PrometheusRegistry();
        MetricsExports.register(registry);
        assertThat(registry.scrape().stream().map(metric -> metric.getMetadata().getPrometheusName()).toList())
                .as("metrics in a scrape of the registry given to MetricsExports.register")
                .containsExactlyInAnyOrder("jvm_memory_direct_bytes_used", "jvm_memory_direct_bytes_max",
                        VERSION_INFO);
    }

    /**
     * Another test of this JVM may have run {@link MetricsExports#initialize()} and so have the version gauge and an
     * open registration of the Netty direct memory gauges on the default registry already. The registration of this
     * call is therefore a mock, and a placeholder takes the version name if nothing has it yet.
     */
    @Test
    public void testRegisterOnTheDefaultRegistryClosesTheGaugeRegistrationWhenTheVersionNameIsTaken() {
        Gauge placeholder = PrometheusRegistry.defaultRegistry.scrape(VERSION_INFO::equals).size() > 0
                ? null
                : Gauge.builder().name(VERSION_INFO).help("-").labelNames("version", "commit")
                        .register(PrometheusRegistry.defaultRegistry);
        NettyDirectMemoryGauges directMemoryGauges = mock(NettyDirectMemoryGauges.class);
        try (MockedStatic<NettyDirectMemoryGauges> gauges = mockStatic(NettyDirectMemoryGauges.class)) {
            gauges.when(NettyDirectMemoryGauges::register).thenReturn(directMemoryGauges);
            assertThatThrownBy(() -> MetricsExports.register(PrometheusRegistry.defaultRegistry))
                    .as("registering on the default registry while %s is taken", VERSION_INFO)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(VERSION_INFO);
        } finally {
            if (placeholder != null) {
                PrometheusRegistry.defaultRegistry.unregister(placeholder);
            }
        }
        verify(directMemoryGauges, description("closing the gauge registration of the call that failed")).close();
    }
}
