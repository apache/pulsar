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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "broker")
public class BacklogQuotaManagerConfigurationTest {

    @Test
    public void testBacklogQuotaConfiguration() {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        double backlogQuotaDefaultLimitGB = 1.6;
        initializeServiceConfiguration(serviceConfiguration, backlogQuotaDefaultLimitGB);
        PulsarService pulsarService = getPulsarService(serviceConfiguration);

        // Test correct conversion of double value
        BacklogQuotaManager backlogQuotaManager = new BacklogQuotaManager(pulsarService);
        assertEquals((long) (backlogQuotaDefaultLimitGB * BacklogQuotaImpl.BYTES_IN_GIGABYTE),
                backlogQuotaManager.getDefaultQuota().getLimitSize());

        int backlogQuotaDefaultLimitBytes = 123;

        // Test precedence of backlogQuotaDefaultLimitGB over backlogQuotaDefaultLimitBytes
        serviceConfiguration.setBacklogQuotaDefaultLimitBytes(backlogQuotaDefaultLimitBytes);
        backlogQuotaManager = new BacklogQuotaManager(pulsarService);
        assertEquals((long) (backlogQuotaDefaultLimitGB * BacklogQuotaImpl.BYTES_IN_GIGABYTE),
                backlogQuotaManager.getDefaultQuota().getLimitSize());

        // Test backlogQuotaDefaultLimitBytes is applied if backlogQuotaDefaultLimitGB <= 0
        serviceConfiguration.setBacklogQuotaDefaultLimitGB(0);
        serviceConfiguration.setBacklogQuotaDefaultLimitBytes(backlogQuotaDefaultLimitBytes);
        backlogQuotaManager = new BacklogQuotaManager(pulsarService);
        assertEquals(backlogQuotaDefaultLimitBytes, backlogQuotaManager.getDefaultQuota().getLimitSize());
    }

    private void initializeServiceConfiguration(ServiceConfiguration serviceConfiguration, double backlogQuotaDefaultLimitGB) {
        serviceConfiguration.setBacklogQuotaDefaultLimitGB(backlogQuotaDefaultLimitGB);
        serviceConfiguration.setClusterName("test");
        serviceConfiguration.setZookeeperServers("http://localhost:2181");
    }

    private PulsarService getPulsarService(ServiceConfiguration serviceConfiguration) {
        PulsarService pulsarService = mock(PulsarService.class);
        ConfigurationCacheService configurationCacheService = mock(ConfigurationCacheService.class);
        when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);
        when(pulsarService.getConfigurationCache()).thenReturn(configurationCacheService);
        return pulsarService;
    }
}
