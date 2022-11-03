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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertSame;
import java.util.Optional;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
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
        configuration.setMetadataStoreUrl("zk:localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(true);
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
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
        init();
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setMetadataStoreUrl("zk:localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(false);
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setBrokerServicePortTls(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setWebServicePortTls(Optional.of(0));
        startBroker(configuration);

        String errorMessage = "Pulsar Function Worker is not enabled, probably functionsWorkerEnabled is set to false";

        int thrownCnt = 0;
        try {
            pulsar.getWorkerService();
        } catch (UnsupportedOperationException e) {
            thrownCnt++;
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.sources().listSources("my", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.sinks().getSinkStatus("my", "test", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.functions().getFunction("my", "test", "test");
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.worker().getClusterLeader();
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        try {
            admin.worker().getFunctionsStats();
        } catch (PulsarAdminException e) {
            thrownCnt++;
            assertEquals(e.getStatusCode(), 409);
            assertEquals(e.getMessage(), errorMessage);
        }

        assertEquals(thrownCnt, 6);
    }

    @Test
    public void testAdvertisedAddress() throws Exception {
        cleanup();
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
        cleanup();
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
        cleanup();
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

    @Test
    public void testBacklogAndRetentionCheck() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setMetadataStoreUrl("memory:local");
        config.setMetadataStoreConfigPath("memory:local");
        PulsarService pulsarService = new PulsarService(config);

        // Check the default configuration
        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        }

        // Only set retention
        config.setDefaultRetentionSizeInMB(5);
        config.setDefaultRetentionTimeInMinutes(5);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        }

        // Set both retention and backlog quota
        config.setBacklogQuotaDefaultLimitBytes(4 * 1024 * 1024);
        config.setBacklogQuotaDefaultLimitSecond(4 * 60);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        }

        // Set invalidated retention and backlog quota
        config.setBacklogQuotaDefaultLimitBytes(6 * 1024 * 1024);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        config.setBacklogQuotaDefaultLimitBytes(4 * 1024 * 1024);
        config.setBacklogQuotaDefaultLimitSecond(6 * 60);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }

        // Only set backlog quota
        config.setDefaultRetentionSizeInMB(0);
        config.setDefaultRetentionTimeInMinutes(0);

        pulsarService = new PulsarService(config);

        try {
            pulsarService.start();
        } catch (Exception e) {
            assertFalse(e.getCause() instanceof IllegalArgumentException);
        }
    }
}
