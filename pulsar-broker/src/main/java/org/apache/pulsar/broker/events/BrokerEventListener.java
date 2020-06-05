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
package org.apache.pulsar.broker.events;

import com.google.common.annotations.Beta;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * A plugin interface that allows you to listen (and possibly mutate) the
 * client requests to the Pulsar brokers.
 *
 * <p>Exceptions thrown by BrokerEventListener methods will be caught, logged, but
 * not propagated further.
 *
 * <p>BrokerEventListener callbacks may be called from multiple threads. Interceptor
 * implementation must ensure thread-safety, if needed.
 */
@Beta
public interface BrokerEventListener extends AutoCloseable {

    /**
     * Called by the broker while new command incoming.
     */
    void onPulsarCommand(BaseCommand command, ServerCnx cnx);

    /**
     * Called by the web service while new request incoming.
     */
    void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException;

    /**
     * Initialize the broker event listener.
     *
     * @throws Exception when fail to initialize the broker event listener.
     */
    void initialize(ServiceConfiguration conf) throws Exception;

    BrokerEventListener DISABLED = new BrokerEventListenerDisabled();

    /**
     * Broker event listener disabled implementation.
     */
    class BrokerEventListenerDisabled implements BrokerEventListener {

        @Override
        public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
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
     * Close this broker event listener.
     */
    @Override
    void close();
}
