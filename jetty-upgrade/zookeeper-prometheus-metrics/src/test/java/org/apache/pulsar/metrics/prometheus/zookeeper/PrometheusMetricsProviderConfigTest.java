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
package org.apache.pulsar.metrics.prometheus.zookeeper;

import io.prometheus.client.CollectorRegistry;
import java.util.Properties;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PrometheusMetricsProviderConfigTest {

    @Test
    public void testInvalidPort() {
        Assert.assertThrows(MetricsProviderLifeCycleException.class, () -> {
            CollectorRegistry.defaultRegistry.clear();
            PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
            Properties configuration = new Properties();
            configuration.setProperty("httpPort", "65536");
            configuration.setProperty("exportJvmInfo", "false");
            provider.configure(configuration);
            provider.start();
        });
    }

    @Test
    public void testInvalidAddr() {
        Assert.assertThrows(MetricsProviderLifeCycleException.class, () -> {
            CollectorRegistry.defaultRegistry.clear();
            PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
            Properties configuration = new Properties();
            configuration.setProperty("httpHost", "master");
            provider.configure(configuration);
            provider.start();
        });
    }

    @Test
    public void testValidConfig() throws MetricsProviderLifeCycleException {
        CollectorRegistry.defaultRegistry.clear();
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        Properties configuration = new Properties();
        configuration.setProperty("httpHost", "0.0.0.0");
        configuration.setProperty("httpPort", "0");
        provider.configure(configuration);
        provider.start();
    }

}
