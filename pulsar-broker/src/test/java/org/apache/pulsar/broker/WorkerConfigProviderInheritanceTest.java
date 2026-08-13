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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.annotations.Test;

/**
 * PIP-478: an embedded (broker-hosted) functions worker inherits the broker's TLS provider pins when its own
 * configuration leaves them unset.
 *
 * <p>Both axes matter and they are different settings: {@code brokerClient*} governs the worker's own
 * outbound worker-to-broker connections, while {@code tlsProvider} / {@code jsseProvider} govern its web
 * listener. Without inheritance a FIPS deployment that pins BCJSSE in {@code broker.conf} got an embedded
 * worker running silently on the platform default — the pin is a security control, so failing it open is the
 * bad direction to fail.
 */
public class WorkerConfigProviderInheritanceTest {

    @Test
    public void embeddedWorkerInheritsProviderPinsFromTheBroker() throws Exception {
        ServiceConfiguration broker = brokerConfig();
        broker.setBrokerClientSslProvider("OPENSSL");
        broker.setBrokerClientJsseProvider("BCJSSE");
        broker.setWebServiceTlsProvider("Conscrypt");
        broker.setJsseProvider("SunJSSE");

        WorkerConfig worker = PulsarService.initializeWorkerConfigFromBrokerConfig(broker, null);

        assertThat(worker.getBrokerClientSslProvider()).isEqualTo("OPENSSL");
        assertThat(worker.getBrokerClientJsseProvider()).isEqualTo("BCJSSE");
        assertThat(worker.getTlsProvider()).isEqualTo("Conscrypt");
        assertThat(worker.getJsseProvider()).isEqualTo("SunJSSE");
    }

    @Test
    public void inheritanceLeavesTheBrokerUnsetCaseAlone() throws Exception {
        WorkerConfig worker = PulsarService.initializeWorkerConfigFromBrokerConfig(brokerConfig(), null);

        assertThat(worker.getBrokerClientSslProvider()).isNull();
        assertThat(worker.getBrokerClientJsseProvider()).isNull();
    }

    private static ServiceConfiguration brokerConfig() {
        ServiceConfiguration config = new ServiceConfiguration();
        // The worker id is built from the configured web port, so it must be set for the call to succeed.
        config.setClusterName("test-cluster");
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(8080));
        return config;
    }
}
