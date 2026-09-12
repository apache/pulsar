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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import io.opentelemetry.api.OpenTelemetry;
import java.lang.reflect.Field;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * Coverage for the V5 {@link PulsarClientBuilder} configuration knobs that don't
 * have observable end-to-end behaviour against a single in-process broker:
 * {@code listenerName}, {@code description}, {@code memoryLimit}, and
 * {@code openTelemetry}. These all just plumb a value into the underlying v4
 * {@code ClientConfigurationData} — so the tests reflect into the wrapped v4
 * client and assert the value made it through.
 *
 * <p>If any of these setters silently dropped the value, the only way it would
 * surface today is by a user reporting that their telemetry collector / broker
 * listener / memory cap doesn't apply. These tests pin the contract.
 */
public class V5ClientBuilderConfigTest extends V5ClientBaseTest {

    @Test
    public void testListenerNamePropagates() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .listenerName("internal")
                .build();

        ClientConfigurationData conf = readV4Conf(client);
        assertEquals(conf.getListenerName(), "internal",
                "listenerName must propagate to the underlying v4 client config");
    }

    @Test
    public void testDescriptionPropagates() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .description("v5-test-client")
                .build();

        ClientConfigurationData conf = readV4Conf(client);
        assertEquals(conf.getDescription(), "v5-test-client",
                "description must propagate to the underlying v4 client config");
    }

    @Test
    public void testMemoryLimitPropagates() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .memoryLimit(MemorySize.ofMegabytes(64))
                .build();

        ClientConfigurationData conf = readV4Conf(client);
        assertEquals(conf.getMemoryLimitBytes(), 64L * 1024 * 1024,
                "memoryLimit must propagate to the underlying v4 client config");
    }

    @Test
    public void testOpenTelemetryPropagates() throws Exception {
        OpenTelemetry custom = OpenTelemetry.noop();
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .openTelemetry(custom)
                .build();

        ClientConfigurationData conf = readV4Conf(client);
        // Same instance — the v4 layer does not clone or wrap the OpenTelemetry handle.
        assertSame(conf.getOpenTelemetry(), custom,
                "openTelemetry instance must be the exact one the user supplied");
    }

    // --- Helpers ---

    private static ClientConfigurationData readV4Conf(PulsarClient v5Client) throws Exception {
        Field f = v5Client.getClass().getDeclaredField("v4Client");
        f.setAccessible(true);
        Object v4Client = f.get(v5Client);
        assertNotNull(v4Client, "expected v4Client on V5 PulsarClient");
        return ((PulsarClientImpl) v4Client).getConfiguration();
    }
}
