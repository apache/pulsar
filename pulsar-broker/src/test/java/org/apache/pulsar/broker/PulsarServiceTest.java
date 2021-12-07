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
import static org.testng.AssertJUnit.assertSame;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarServiceTest extends MockedPulsarServiceBaseTest {

    private boolean useStaticPorts = false;

    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        useStaticPorts = false;
        resetConfig();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        if (useStaticPorts) {
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
    public void testAdvertisedAddress() throws Exception {
        useStaticPorts = true;
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(pulsar.getBrokerServiceUrlTls(), "pulsar+ssl://localhost:6651");
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://localhost:6660");
        assertEquals(pulsar.getWebServiceAddress(), "http://localhost:8081");
        assertEquals(pulsar.getWebServiceAddressTls(), "https://localhost:8082");
        assertEquals(conf, pulsar.getConfiguration());
    }

    @Test
    public void testAdvertisedListeners() throws Exception {
        // don't use dynamic ports when using advertised listeners (#12079)
        useStaticPorts = true;
        conf.setAdvertisedListeners("internal:pulsar://gateway:6650, internal:pulsar+ssl://gateway:6651");
        conf.setInternalListenerName("internal");
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(pulsar.getBrokerServiceUrlTls(), "pulsar+ssl://gateway:6651");
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://gateway:6650");
        assertEquals(pulsar.getWebServiceAddress(), "http://localhost:8081");
        assertEquals(pulsar.getWebServiceAddressTls(), "https://localhost:8082");
        assertEquals(conf, pulsar.getConfiguration());
    }

    @Test
    public void testDynamicBrokerPort() throws Exception {
        useStaticPorts = false;
        setup();
        assertEquals(pulsar.getAdvertisedAddress(), "localhost");
        assertEquals(conf, pulsar.getConfiguration());
        assertEquals(conf.getBrokerServicePortTls(), pulsar.getBrokerListenPortTls());
        assertEquals(conf.getBrokerServicePort(), pulsar.getBrokerListenPort());
        assertEquals(pulsar.getBrokerServiceUrlTls(), "pulsar+ssl://localhost:" + pulsar.getBrokerListenPortTls().get());
        assertEquals(pulsar.getBrokerServiceUrl(), "pulsar://localhost:" + pulsar.getBrokerListenPort().get());
        assertEquals(pulsar.getWebServiceAddress(), "http://localhost:" + pulsar.getWebService().getListenPortHTTP().get());
        assertEquals(pulsar.getWebServiceAddressTls(), "https://localhost:" + pulsar.getWebService().getListenPortHTTPS().get());
    }

}
