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
package org.apache.pulsar;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.ClusterResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarStandaloneTest {

    @Test
    public void testCreateNameSpace() throws Exception {
        final String cluster = "cluster1";
        final String tenant = "tenant1";
        final NamespaceName ns = NamespaceName.get(tenant, "ns1");

        ClusterResources cr = mock(ClusterResources.class);
        when(cr.clusterExists(cluster)).thenReturn(false).thenReturn(true);
        doNothing().when(cr).createCluster(eq(cluster), any());

        TenantResources tr = mock(TenantResources.class);
        when(tr.tenantExists(tenant)).thenReturn(false).thenReturn(true);
        doNothing().when(tr).createTenant(eq(tenant), any());

        NamespaceResources nsr = mock(NamespaceResources.class);
        when(nsr.namespaceExists(ns)).thenReturn(false).thenReturn(true);
        doNothing().when(nsr).createPolicies(eq(ns), any());

        PulsarResources resources = mock(PulsarResources.class);
        when(resources.getClusterResources()).thenReturn(cr);
        when(resources.getTenantResources()).thenReturn(tr);
        when(resources.getNamespaceResources()).thenReturn(nsr);

        PulsarService broker = mock(PulsarService.class);
        when(broker.getPulsarResources()).thenReturn(resources);
        when(broker.getWebServiceAddress()).thenReturn("pulsar://localhost:8080");
        when(broker.getWebServiceAddressTls()).thenReturn(null);
        when(broker.getBrokerServiceUrl()).thenReturn("pulsar://localhost:6650");
        when(broker.getBrokerServiceUrlTls()).thenReturn(null);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName(cluster);

        PulsarStandalone standalone = new PulsarStandalone();
        standalone.setBroker(broker);
        standalone.setConfig(config);

        standalone.createNameSpace(cluster, tenant, ns);
        standalone.createNameSpace(cluster, tenant, ns);
        verify(cr, times(1)).createCluster(eq(cluster), any());
        verify(tr, times(1)).createTenant(eq(tenant), any());
        verify(nsr, times(1)).createPolicies(eq(ns), any());
    }

}
