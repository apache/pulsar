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
package org.apache.pulsar.broker.web;

import lombok.SneakyThrows;
import org.apache.pulsar.common.intercept.InterceptException;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Response;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

/**
 * Unit test for ExceptionHandler.
 */
@Test(groups = "broker")
public class ExceptionHandlerTest {

    @Test
    @SneakyThrows
    public void testHandle() {
        ExceptionHandler handler = new ExceptionHandler();
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        handler.handle(response, new InterceptException(HttpStatus.PRECONDITION_FAILED_412, "Reach the max tenants [5] restriction"));
        Mockito.verify(response).sendError(Mockito.anyInt(), Mockito.anyString());
        handler.handle(response, new InterceptException(HttpStatus.INTERNAL_SERVER_ERROR_500, "internal exception"));
        Mockito.verify(response, Mockito.times(2)).sendError(Mockito.anyInt(), Mockito.anyString());
        handler.handle(response, new IllegalArgumentException("illegal argument exception "));
        Mockito.verify(response, Mockito.times(3)).sendError(Mockito.anyInt(), Mockito.anyString());
        Response response2 = Mockito.mock(Response.class);
        HttpChannel httpChannel = Mockito.mock(HttpChannel.class);
        Mockito.when(response2.getHttpChannel()).thenReturn(httpChannel);
        handler.handle(response2, new InterceptException(HttpStatus.PRECONDITION_FAILED_412, "Reach the max tenants [5] restriction"));
        Mockito.verify(httpChannel).sendResponse(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

}
