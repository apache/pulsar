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

/**
 * Pulsar broker interceptor.
 */
package org.apache.pulsar.proxy.server.interceptor;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import java.io.IOException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.PulsarApi;

public interface ProxyInterceptor {
    /**
     * Called by the broker while new command incoming.
     */
    void onPulsarCommand(PulsarApi.BaseCommand command, ServerCnx cnx) throws Exception;

    /**
     * Called by the web service while new request incoming.
     */
    void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException;

    /**
     * Initialize the broker interceptor.
     *
     * @throws Exception when fail to initialize the broker interceptor.
     */
    void initialize(ServiceConfiguration conf) throws Exception;

    ProxyInterceptor DISABLED = new BrokerInterceptorDisabled();

    /**
     * Broker interceptor disabled implementation.
     */
    class BrokerInterceptorDisabled implements ProxyInterceptor {

        @Override
        public void onPulsarCommand(PulsarApi.BaseCommand command, ServerCnx cnx) throws Exception {
            //No-op
        }

        @Override
        public void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            chain.doFilter(request, response);
        }

        @Override
        public void initialize(ServiceConfiguration conf) throws Exception {
            //No-op
        }

        @Override
        public void close() {
            //No-op
        }
    }

    /**
     * Close this broker interceptor.
     */
    @Override
    void close();
}
