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

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.BaseCommand;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * A collection of broker interceptor.
 */
@Slf4j
public class BrokerInterceptors implements BrokerInterceptor {

    private final Map<String, BrokerInterceptorWithClassLoader> interceptors;

    public BrokerInterceptors(Map<String, BrokerInterceptorWithClassLoader> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Load the broker event interceptor for the given <tt>interceptor</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of broker event interceptor
     */
    public static BrokerInterceptor load(ServiceConfiguration conf) throws IOException {
        BrokerInterceptorDefinitions definitions =
                BrokerInterceptorUtils.searchForInterceptors(conf.getBrokerInterceptorsDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, BrokerInterceptorWithClassLoader> builder = ImmutableMap.builder();

        conf.getBrokerInterceptors().forEach(interceptorName -> {

            BrokerInterceptorMetadata definition = definitions.interceptors().get(interceptorName);
            if (null == definition) {
                throw new RuntimeException("No broker interceptor is found for name `" + interceptorName
                        + "`. Available broker interceptors are : " + definitions.interceptors());
            }

            BrokerInterceptorWithClassLoader interceptor;
            try {
                interceptor = BrokerInterceptorUtils.load(definition, conf.getNarExtractionDirectory());
                if (interceptor != null) {
                    builder.put(interceptorName, interceptor);
                }
                log.info("Successfully loaded broker interceptor for name `{}`", interceptorName);
            } catch (IOException e) {
                log.error("Failed to load the broker interceptor for name `" + interceptorName + "`", e);
                throw new RuntimeException("Failed to load the broker interceptor for name `" + interceptorName + "`");
            }
        });

        Map<String, BrokerInterceptorWithClassLoader> interceptors = builder.build();
        if (interceptors != null && !interceptors.isEmpty()) {
            return new BrokerInterceptors(interceptors);
        } else {
            return DISABLED;
        }
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws Exception {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onPulsarCommand(command, cnx);
        }
    }

    @Override
    public void onWebServiceRequest(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        for (BrokerInterceptorWithClassLoader value : interceptors.values()) {
            value.onWebServiceRequest(request, response, chain);
        }
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        for (BrokerInterceptorWithClassLoader v : interceptors.values()) {
            v.initialize(conf);
        }
    }

    @Override
    public void close() {
        interceptors.values().forEach(BrokerInterceptorWithClassLoader::close);
    }
}
