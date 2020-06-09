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

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;
import org.apache.pulsar.common.nar.NarClassLoader;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * A broker interceptor with it's classloader.
 */
@Slf4j
@Data
@RequiredArgsConstructor
public class BrokerInterceptorWithClassLoader implements BrokerInterceptor {

    private final BrokerInterceptor interceptor;
    private final NarClassLoader classLoader;

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws Exception {
        this.interceptor.onPulsarCommand(command, cnx);
    }

    @Override
    public void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        this.interceptor.onWebServiceRequest(request, response, chain);
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        this.interceptor.initialize(conf);
    }

    @Override
    public void close() {
        interceptor.close();
        try {
            classLoader.close();
        } catch (IOException e) {
            log.warn("Failed to close the broker interceptor class loader", e);
        }
    }
}
