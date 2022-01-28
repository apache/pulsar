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
package org.apache.pulsar.broker.intercept;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.intercept.InterceptException;

public class MockBrokerInterceptor implements BrokerInterceptor {

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {
        // no-op
    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {
        // no-op
    }

    @Override
    public void onWebserviceRequest(ServletRequest request) {
        // no-op
    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response) throws IOException, ServletException {
        // no-op
    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
