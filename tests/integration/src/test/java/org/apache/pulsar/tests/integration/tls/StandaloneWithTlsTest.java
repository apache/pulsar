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

package org.apache.pulsar.tests.integration.tls;

import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.tests.integration.suites.PulsarStandaloneTestSuite;
import org.testng.annotations.Test;

public class StandaloneWithTlsTest extends PulsarStandaloneTestSuite {

    @Override
    public void setUpCluster() throws Exception {
        setupTLS();
        standaloneEnvs.put("PULSAR_PREFIX_loadManagerClassName","org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl");
        super.setUpCluster();
    }

    @Test
    public void testTlsTransportByAuthenticationTls() throws PulsarAdminException, PulsarClientException {
        @Cleanup PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(container.getHttpsServiceUrl())
                .tlsTrustCertsFilePath(clientTlsTrustCertsFilePath)
                .authentication(new AuthenticationTls(clientTlsCertificateFilePath, clientTlsKeyFilePath))
                .build();

        admin.functions().getBuiltInFunctions();
    }
}
