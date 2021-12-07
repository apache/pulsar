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
package org.apache.pulsar.websocket;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;
import org.testng.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * WebSocketHttpServletRequestWrapper test.
 */
public class WebSocketHttpServletRequestWrapperTest {

    private static final String TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.U387jG"
            + "-gmpEXNmTjbnnnk24jCXnfy7OTiQhhhOdXPgV2wEvZYr83KRSmH54wJQr4V2FCWIFb_6mBc_"
            + "E2acpfpfBOTTzrtietfhd6wE5uOP2NXaLpy_kUDsE3ZQGKPEsn18cWQUw54GAzS1oRcG9TnoqSCSFFGabvo"
            + "FTiOMHoBQ3ZHO3TqAGqlJlRF5ZXMkRtQ9vwbPC-mlwIfRrRIJfK5_ijPRkpgFSEvAwp0rX6roz08SyTj_"
            + "d4UNT96nsEL6sRNTpZMQ0qNj2_LMKFnwF3O_xe43-Uen3TllkAzhNd9Z6qIxyJyFbaFyWAVgiAfoFWQD0v4EmV96ZzKZvv3CbGjw";
    private static final String BEARER_TOKEN = WebSocketHttpServletRequestWrapper.HTTP_HEADER_VALUE_PREFIX + TOKEN;

    @Test
    public void testTokenParamWithBearerPrefix() {
        UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
        Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
                .thenReturn(BEARER_TOKEN);

        WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
                new WebSocketHttpServletRequestWrapper(httpServletRequest);
        Assert.assertEquals(
                webSocketHttpServletRequestWrapper.getHeader(WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME),
                BEARER_TOKEN);
    }

    @Test
    public void testTokenParamWithOutBearerPrefix() {
        UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
        Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
                .thenReturn(TOKEN);

        WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
                new WebSocketHttpServletRequestWrapper(httpServletRequest);
        Assert.assertEquals(
                webSocketHttpServletRequestWrapper.getHeader(WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME),
                BEARER_TOKEN);
    }

    @Test
    public void mockRequestTest() throws Exception {
        WebSocketProxyConfiguration config = PulsarConfigurationLoader.create(
                this.getClass().getClassLoader().getResource("websocket.conf").getFile(),
                WebSocketProxyConfiguration.class);
        String publicKeyPath = "file://" + this.getClass().getClassLoader().getResource("my-public.key").getFile();
        config.getProperties().setProperty("tokenPublicKey", publicKeyPath);
        WebSocketService service = new WebSocketService(config);
        service.start();

        UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
        Mockito.when(httpServletRequest.getRemoteAddr()).thenReturn("localhost");
        Mockito.when(httpServletRequest.getRemotePort()).thenReturn(8080);
        Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
                .thenReturn(TOKEN);
        WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
                new WebSocketHttpServletRequestWrapper(httpServletRequest);

        Assert.assertEquals(service.getAuthenticationService().authenticateHttpRequest(webSocketHttpServletRequestWrapper),
                "test-user");
    }

}
