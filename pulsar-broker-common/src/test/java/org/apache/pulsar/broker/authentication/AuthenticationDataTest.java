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
package org.apache.pulsar.broker.authentication;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AuthenticationDataTest {

    HttpServletRequest request;

    @BeforeMethod
    void setup(){
        request = mock(HttpServletRequest.class);
        doReturn("127.0.0.1").when(request).getRemoteAddr();
        doReturn(8080).when(request).getRemotePort();
    }

    @Test
    public void testAuthenticationDataHttp() {
        AuthenticationDataHttp data = new AuthenticationDataHttp(request);
        assertEquals(data.getProperty("hi"), null);
        data.setProperty("hi", "hello");
        assertEquals(data.getProperty("hi"), "hello");
    }

    @Test
    public void testAuthenticationDataHttps() {
        AuthenticationDataHttps data = new AuthenticationDataHttps(request);
        assertEquals(data.getProperty("hi"), null);
        data.setProperty("hi", "hello");
        assertEquals(data.getProperty("hi"), "hello");
    }
}
