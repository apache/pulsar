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

import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * WebSocketHttpServletRequestWrapper test.
 */
public class WebSocketHttpServletRequestWrapperTest {

    private final static String BEARER_TOKEN = WebSocketHttpServletRequestWrapper.HTTP_HEADER_VALUE_PREFIX
            + "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.U387jG";

    @Test
    public void testTokenParamWithBearerPrefix() {
        UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
        Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
                .thenReturn(BEARER_TOKEN);

        WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
                new WebSocketHttpServletRequestWrapper(httpServletRequest);
        String token = webSocketHttpServletRequestWrapper.getHeader(
                WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME);
        Assert.assertEquals(BEARER_TOKEN, token);
    }

    @Test
    public void testTokenParamWithOutBearerPrefix() {
        String bearerToken = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.U387jG";
        UpgradeHttpServletRequest httpServletRequest = Mockito.mock(UpgradeHttpServletRequest.class);
        Mockito.when(httpServletRequest.getParameter(WebSocketHttpServletRequestWrapper.TOKEN))
                .thenReturn(bearerToken);

        WebSocketHttpServletRequestWrapper webSocketHttpServletRequestWrapper =
                new WebSocketHttpServletRequestWrapper(httpServletRequest);
        String token = webSocketHttpServletRequestWrapper.getHeader(
                WebSocketHttpServletRequestWrapper.HTTP_HEADER_NAME);
        Assert.assertEquals(BEARER_TOKEN, token);
    }

}
