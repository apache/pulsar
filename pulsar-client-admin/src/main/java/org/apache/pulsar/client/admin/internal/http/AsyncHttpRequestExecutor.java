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
package org.apache.pulsar.client.admin.internal.http;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;

/**
 * Interface for executing HTTP requests asynchronously.
 * This is used internally in the Pulsar Admin client for executing HTTP requests that by-pass the Jersey client
 * and use the AsyncHttpClient API directly.
 */
public interface AsyncHttpRequestExecutor {
    /**
     * Execute the given HTTP request asynchronously.
     *
     * @param request the HTTP request to execute
     * @return a future that will be completed with the HTTP response
     */
    CompletableFuture<Response> executeRequest(Request request);
    /**
     * Execute the given HTTP request asynchronously.
     *
     * @param request the HTTP request to execute
     * @param handlerSupplier a supplier for the async handler to use for the request
     * @return a future that will be completed with the HTTP response
     */
    CompletableFuture<Response> executeRequest(Request request, Supplier<AsyncHandler<Response>> handlerSupplier);
}
