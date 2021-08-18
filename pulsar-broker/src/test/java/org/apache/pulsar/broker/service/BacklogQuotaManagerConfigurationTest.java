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
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

@Test(groups = "broker")
public class BacklogQuotaManagerConfigurationTest {

    @Test
    public void testBacklogQuotaDefaultLimitGBConversion() {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        initializeServiceConfiguration(serviceConfiguration);

        PulsarService pulsarService = getPulsarService(serviceConfiguration);
        BacklogQuotaManager backlogQuotaManager = new BacklogQuotaManager(pulsarService);

        assertEquals(backlogQuotaManager.getDefaultQuota().getLimitSize(), 1717986918);
    }

    @Test
    public void testBacklogQuotaDefaultLimitPrecedence() {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        initializeServiceConfiguration(serviceConfiguration);
        serviceConfiguration.setBacklogQuotaDefaultLimitBytes(123);

        PulsarService pulsarService = getPulsarService(serviceConfiguration);
        BacklogQuotaManager backlogQuotaManager = new BacklogQuotaManager(pulsarService);

        assertEquals(backlogQuotaManager.getDefaultQuota().getLimitSize(), 1717986918);
    }

    @Test
    public void testBacklogQuotaDefaultLimitBytes() {
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        initializeServiceConfiguration(serviceConfiguration);
        serviceConfiguration.setBacklogQuotaDefaultLimitGB(0);
        serviceConfiguration.setBacklogQuotaDefaultLimitBytes(123);

        PulsarService pulsarService = getPulsarService(serviceConfiguration);
        BacklogQuotaManager backlogQuotaManager = new BacklogQuotaManager(pulsarService);

        assertEquals(backlogQuotaManager.getDefaultQuota().getLimitSize(), 123);
    }

    private void initializeServiceConfiguration(ServiceConfiguration serviceConfiguration) {
        serviceConfiguration.setBacklogQuotaDefaultLimitGB(1.6);
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
