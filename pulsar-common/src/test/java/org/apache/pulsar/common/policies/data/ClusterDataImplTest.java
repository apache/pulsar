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
package org.apache.pulsar.common.policies.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import org.apache.pulsar.client.api.ProxyProtocol;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;

public class ClusterDataImplTest {

    @Test
    public void verifyClone() {
        ClusterDataImpl originalData = ClusterDataImpl.builder()
                .serviceUrl("pulsar://test")
                .serviceUrlTls("pulsar+ssl://test")
                .brokerServiceUrl("pulsar://test:6650")
                .brokerServiceUrlTls("pulsar://test:6651")
                .proxyServiceUrl("pulsar://proxy:6650")
                .authenticationPlugin("test-plugin")
                .authenticationParameters("test-params")
                .proxyProtocol(ProxyProtocol.SNI)
                .peerClusterNames(new LinkedHashSet<>())
                .brokerClientTlsEnabled(true)
                .tlsAllowInsecureConnection(false)
                .brokerClientTlsEnabledWithKeyStore(true)
                .brokerClientTlsTrustStoreType("JKS")
                .brokerClientTlsTrustStore("/my/trust/store")
                .brokerClientTlsTrustStorePassword("some-password")
                .brokerClientTlsKeyStoreType("PCKS12")
                .brokerClientTlsKeyStore("/my/key/store")
                .brokerClientTlsKeyStorePassword("a-different-password")
                .brokerClientTrustCertsFilePath("/my/trusted/certs")
                .brokerClientKeyFilePath("/my/key/file")
                .brokerClientCertificateFilePath("/my/cert/file")
                .listenerName("a-listener")
                .migrated(true)
                .migratedClusterUrl(new ClusterData.ClusterUrl("pulsar://remote", "pulsar+ssl://remote"))
                .build();

        ClusterDataImpl clone = originalData.clone().build();

        assertEquals(clone, originalData, "Clones should have object equality.");
        assertNotSame(clone, originalData, "Clones should not be the same reference.");
    }
}
