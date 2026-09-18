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
package org.apache.pulsar.client.impl.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.impl.auth.v5.HttpChallengeTransport;
import org.testng.annotations.Test;

/**
 * PIP-478: the SASL-over-HTTP transport must close every JAX-RS {@link Response} it completes from.
 *
 * <p>{@code InvocationCallback<Response>} hands the caller an unclosed response, and reading only its
 * headers neither consumes the entity nor releases the connection. The authentication driver runs at least
 * one round on every admin request, so a response left open leaks a pooled connection per request rather
 * than per client — which is why the completion path, not only the timed-out path, has to close.
 */
public class JaxRsResponseCloseTest {

    @Test
    public void completingFromAResponseAlsoClosesIt() {
        Response response = responseWith(401, "WWW-Authenticate", "Negotiate");
        CompletableFuture<HttpChallengeTransport.Result> future = new CompletableFuture<>();

        AuthenticationSasl.completeAndClose(future, response);

        assertThat(future).isCompleted();
        HttpChallengeTransport.Result result = future.join();
        assertThat(result.statusCode()).isEqualTo(401);
        // HttpAuthHeaders normalises names, since HTTP header names are case-insensitive.
        assertThat(result.responseHeaders().asMap()).containsEntry("www-authenticate", "Negotiate");
        verify(response, times(1)).close();
    }

    /**
     * The response has to be released even when the future has already been completed elsewhere — the round
     * timed out, or the request was cancelled and the peer answered afterwards. Completing a settled future
     * is a no-op, so without the close in a finally the late response would leak silently.
     */
    @Test
    public void aResponseArrivingAfterTheFutureSettledIsStillClosed() {
        Response response = responseWith(200, "X-Ignored", "value");
        CompletableFuture<HttpChallengeTransport.Result> future = new CompletableFuture<>();
        future.completeExceptionally(new IllegalStateException("round already timed out"));

        AuthenticationSasl.completeAndClose(future, response);

        assertThat(future).isCompletedExceptionally();
        verify(response, times(1)).close();
    }

    private static Response responseWith(int status, String headerName, String headerValue) {
        Response response = mock(Response.class);
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.putSingle(headerName, headerValue);
        when(response.getStatus()).thenReturn(status);
        when(response.getStringHeaders()).thenReturn(headers);
        return response;
    }
}
