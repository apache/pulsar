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
package org.apache.pulsar.broker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertSame;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


@Slf4j
public class PulsarServiceTest extends MockedPulsarServiceBaseTest {

    private boolean useListenerName = false;

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        useListenerName = false;
        resetConfig();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        if (useListenerName) {
            conf.setAdvertisedAddress(null);
            conf.setBrokerServicePortTls(Optional.of(6651));
            conf.setBrokerServicePort(Optional.of(6660));
            conf.setWebServicePort(Optional.of(8081));
            conf.setWebServicePortTls(Optional.of(8082));
        }
    }

    @Test
    public void testGetWorkerService() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setZookeeperServers("localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(true);
        configuration.setBrokerShutdownTimeoutMs(0L);
        WorkerService expectedWorkerService = mock(WorkerService.class);
        @Cleanup
        PulsarService pulsarService = spy(new PulsarService(configuration, new WorkerConfig(),
                Optional.of(expectedWorkerService), (exitCode) -> {}));

        WorkerService actualWorkerService = pulsarService.getWorkerService();
        assertSame(expectedWorkerService, actualWorkerService);
    }

    /**
     * Verifies that the getWorkerService throws {@link UnsupportedOperationException}
     * when functionsWorkerEnabled is set to false .
     */
    @Test
    public void testGetWorkerServiceException() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setZookeeperServers("localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(false);
        configuration.setBrokerShutdownTimeoutMs(0L);
        @Cleanup
        PulsarService pulsarService = new PulsarService(configuration, new WorkerConfig(),
                Optional.empty(), (exitCode) -> {});

        String errorMessage = "Pulsar Function Worker is not enabled, probably functionsWorkerEnabled is set to false";
        try {
            pulsarService.getWorkerService();
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), errorMessage);
        }
    }

    @Test
    public void testAppliedAdvertised() throws Exception {
        useListenerName = true;
        conf.setAdvertisedListeners("internal:pulsar://127.0.0.1:6650, internal:pulsar+ssl://127.0.0.1:6651");
        conf.setInternalListenerName("internal");
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "127.0.0.1");
        assertNull(pulsar.getConfiguration().getAdvertisedAddress());
        assertEquals(conf, pulsar.getConfiguration());

        cleanup();
        resetConfig();
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(conf, pulsar.getConfiguration());
        assertEquals(pulsar.brokerUrlTls(conf), "pulsar+ssl://localhost:" + pulsar.getBrokerListenPortTls().get());
        assertEquals(pulsar.brokerUrl(conf), "pulsar://localhost:" + pulsar.getBrokerListenPort().get());
        assertEquals(pulsar.webAddress(conf), "http://localhost:" + pulsar.getWebService().getListenPortHTTP().get());
        assertEquals(pulsar.webAddressTls(conf), "https://localhost:" + pulsar.getWebService().getListenPortHTTPS().get());
    }

}
