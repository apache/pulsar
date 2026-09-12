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

import static org.testng.AssertJUnit.assertEquals;
import io.prometheus.client.CollectorRegistry;
import java.util.Properties;
import org.testng.annotations.Test;

/**
 * Tests about Prometheus Metrics Provider. Please note that we are not testing
 * Prometheus but our integration.
 */
public class ExportJvmInfoTest {

    @Test
    public void exportInfo() throws Exception {
        runTest(true);
    }

    @Test
    public void doNotExportInfo() throws Exception {
        runTest(false);
    }

    private void runTest(boolean exportJvmInfo) throws Exception {
        CollectorRegistry.defaultRegistry.clear();
        PrometheusMetricsProvider provider = new PrometheusMetricsProvider();
        try {
            Properties configuration = new Properties();
            configuration.setProperty("httpPort", "0"); // ephemeral port
            configuration.setProperty("exportJvmInfo", exportJvmInfo + "");
            provider.configure(configuration);
            provider.start();
            boolean[] found = {false};
            provider.dump((k, v) -> {
                found[0] = found[0] || k.contains("heap");
            });
            assertEquals(exportJvmInfo, found[0]);
        } finally {
            provider.stop();
        }
    }

}
