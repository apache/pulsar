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
package org.apache.pulsar.client.admin.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.Map;

public class PulsarAdminBuilderImplTest {

    @Test
    public void testAdminBuilderWithServiceUrlNotSet() throws PulsarClientException {
        try{
            PulsarAdmin.builder().build();
            fail();
        } catch (IllegalArgumentException exception) {
            assertEquals("Service URL needs to be specified", exception.getMessage());
        }
    }

    @Test
    public void testGetPropertiesFromConf() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serviceUrl", "pulsar://localhost:6650");
        config.put("requestTimeoutMs", 10);
        config.put("autoCertRefreshSeconds", 20);
        config.put("connectionTimeoutMs", 30);
        config.put("readTimeoutMs", 40);
        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder().loadConf(config);
        PulsarAdminImpl admin = (PulsarAdminImpl) adminBuilder.build();
        ClientConfigurationData clientConfigData = admin.getClientConfigData();
        Assert.assertEquals(clientConfigData.getRequestTimeoutMs(), 10);
        Assert.assertEquals(clientConfigData.getAutoCertRefreshSeconds(), 20);
        Assert.assertEquals(clientConfigData.getConnectionTimeoutMs(), 30);
        Assert.assertEquals(clientConfigData.getReadTimeoutMs(), 40);
    }
}
