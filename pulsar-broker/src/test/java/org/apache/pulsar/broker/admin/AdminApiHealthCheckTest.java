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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicVersion;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;

@Test(groups = "broker")
@Slf4j
public class AdminApiHealthCheckTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        resetConfig();
        super.internalSetup();
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(
                Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("pulsar", tenantInfo);
        admin.namespaces().createNamespace("pulsar/system", Sets.newHashSet("test"));
        admin.tenants().createTenant("public", tenantInfo);
        admin.namespaces().createNamespace("public/default", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testHealthCheckup() throws Exception {
        admin.brokers().healthcheck();
    }

    @Test
    public void testHealthCheckupV1() throws Exception {
        admin.brokers().healthcheck(TopicVersion.V1);
    }

    @Test(expectedExceptions = PulsarAdminException.class)
    public void testHealthCheckupV2Error() throws Exception {
        admin.brokers().healthcheck(TopicVersion.V2);
    }

    @Test
    public void testHealthCheckupV2() throws Exception {
        final URL pulsarWebAddress = new URL(pulsar.getWebServiceAddress());
        final String targetNameSpace = "pulsar/" +
                pulsarWebAddress.getHost() + ":" + pulsarWebAddress.getPort();
        log.info("Target namespace for broker admin healthcheck V2 endpoint is {}", targetNameSpace);
        admin.namespaces().createNamespace(targetNameSpace);
        admin.brokers().healthcheck(TopicVersion.V2);
    }
}
