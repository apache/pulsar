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
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Response;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

import static org.eclipse.jetty.http.HttpStatus.INTERNAL_SERVER_ERROR_500;
import static org.eclipse.jetty.http.HttpStatus.PRECONDITION_FAILED_412;

/**
 * Unit test for ExceptionHandler.
 */
@Test(groups = "broker")
public class ExceptionHandlerTest {

    @Test
    @SneakyThrows
    public void testHandle() {
        String restriction = "Reach the max tenants [5] restriction";
        String internal = "internal exception";
        String illegal = "illegal argument exception ";
        ExceptionHandler handler = new ExceptionHandler();
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        handler.handle(response, new InterceptException(PRECONDITION_FAILED_412, restriction));
        Mockito.verify(response).sendError(PRECONDITION_FAILED_412, restriction);

        handler.handle(response, new InterceptException(INTERNAL_SERVER_ERROR_500, internal));
        Mockito.verify(response).sendError(INTERNAL_SERVER_ERROR_500, internal);

        handler.handle(response, new IllegalArgumentException(illegal));
        Mockito.verify(response).sendError(INTERNAL_SERVER_ERROR_500, illegal);

        Response response2 = Mockito.mock(Response.class);
        HttpChannel httpChannel = Mockito.mock(HttpChannel.class);
        Mockito.when(response2.getHttpChannel()).thenReturn(httpChannel);
        handler.handle(response2, new InterceptException(PRECONDITION_FAILED_412, restriction));
        Mockito.verify(httpChannel).sendResponse(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    }

}
