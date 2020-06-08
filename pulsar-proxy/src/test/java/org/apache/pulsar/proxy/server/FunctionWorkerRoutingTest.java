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
package org.apache.pulsar.proxy.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FunctionWorkerRoutingTest {

    @Test
    public void testFunctionWorkerRedirect() throws Exception {
        String functionWorkerUrl = "http://function";
        String brokerUrl = "http://broker";

        ProxyConfiguration proxyConfig = new ProxyConfiguration();
        proxyConfig.setBrokerWebServiceURL(brokerUrl);
        proxyConfig.setFunctionWorkerWebServiceURL(functionWorkerUrl);

        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        AdminProxyHandler handler = new AdminProxyHandler(proxyConfig, discoveryProvider);

        String funcUrl = handler.rewriteTarget(buildRequest("/admin/v3/functions/test/test"));
        Assert.assertEquals(funcUrl, String.format("%s/admin/v3/functions/%s/%s",
                functionWorkerUrl, "test", "test"));

        String sourceUrl = handler.rewriteTarget(buildRequest("/admin/v3/sources/test/test"));
        Assert.assertEquals(sourceUrl, String.format("%s/admin/v3/sources/%s/%s",
                functionWorkerUrl, "test", "test"));

        String sinkUrl = handler.rewriteTarget(buildRequest("/admin/v3/sinks/test/test"));
        Assert.assertEquals(sinkUrl, String.format("%s/admin/v3/sinks/%s/%s",
                functionWorkerUrl, "test", "test"));

        String tenantUrl = handler.rewriteTarget(buildRequest("/admin/v2/tenants/test"));
        Assert.assertEquals(tenantUrl, String.format("%s/admin/v2/tenants/%s",
                brokerUrl, "test"));
    }

    static HttpServletRequest buildRequest(String url) {
        HttpServletRequest mockReq = mock(HttpServletRequest.class);
        when(mockReq.getRequestURI()).thenReturn(url);
        return mockReq;
    }

}
