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
package org.apache.pulsar.proxy.server.protocol;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;

@Slf4j
public class ProxyProtocols implements AutoCloseable {
    private final Map<String, ProxyProtocolWithClassLoader> interceptors;

    public ProxyProtocols(Map<String, ProxyProtocolWithClassLoader> interceptors) {
        this.interceptors = interceptors;
    }

    /**
     * Load the broker event interceptor for the given <tt>interceptor</tt> list.
     *
     * @param conf the pulsar broker service configuration
     * @return the collection of broker event interceptor
     */
    public static ProxyProtocols load(ServiceConfiguration conf) throws IOException {
        ProxyProtocolDefinitions definitions =
                ProxyProtocolUtils.searchForInterceptors(conf.getBrokerInterceptorsDirectory(), conf.getNarExtractionDirectory());

        ImmutableMap.Builder<String, ProxyProtocolWithClassLoader> builder = ImmutableMap.builder();

        conf.getBrokerInterceptors().forEach(interceptorName -> {

            ProxyProtocolMetadata definition = definitions.interceptors().get(interceptorName);
            if (null == definition) {
                throw new RuntimeException("No broker interceptor is found for name `" + interceptorName
                        + "`. Available broker interceptors are : " + definitions.interceptors());
            }

            ProxyProtocolWithClassLoader interceptor;
            try {
                interceptor = ProxyProtocolUtils.load(definition, conf.getNarExtractionDirectory());
                if (interceptor != null) {
                    builder.put(interceptorName, interceptor);
                }
                log.info("Successfully loaded broker interceptor for name `{}`", interceptorName);
            } catch (IOException e) {
                log.error("Failed to load the broker interceptor for name `" + interceptorName + "`", e);
                throw new RuntimeException("Failed to load the broker interceptor for name `" + interceptorName + "`");
            }
        });

        Map<String, ProxyProtocolWithClassLoader> interceptors = builder.build();
        if (interceptors != null && !interceptors.isEmpty()) {
            return new ProxyProtocols(interceptors);
        }

        return null;
    }

    @Override
    public void close() {
        interceptors.values().forEach(ProxyProtocolWithClassLoader::close);
    }
}
