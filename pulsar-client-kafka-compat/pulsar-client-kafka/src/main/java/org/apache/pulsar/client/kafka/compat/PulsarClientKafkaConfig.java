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
package org.apache.pulsar.client.kafka.compat;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;

public class PulsarClientKafkaConfig {

    /// Config variables
    public static final String AUTHENTICATION_CLASS = "pulsar.authentication.class";
    public static final String AUTHENTICATION_PARAMS_MAP = "pulsar.authentication.params.map";
    public static final String AUTHENTICATION_PARAMS_STRING = "pulsar.authentication.params.string";
    public static final String USE_TLS = "pulsar.use.tls";
    public static final String TLS_TRUST_CERTS_FILE_PATH = "pulsar.tls.trust.certs.file.path";
    public static final String TLS_ALLOW_INSECURE_CONNECTION = "pulsar.tls.allow.insecure.connection";
    public static final String TLS_HOSTNAME_VERIFICATION = "pulsar.tls.hostname.verification";

    public static final String OPERATION_TIMEOUT_MS = "pulsar.operation.timeout.ms";
    public static final String STATS_INTERVAL_SECONDS = "pulsar.stats.interval.seconds";
    public static final String NUM_IO_THREADS = "pulsar.num.io.threads";

    public static final String CONNECTIONS_PER_BROKER = "pulsar.connections.per.broker";

    public static final String USE_TCP_NODELAY = "pulsar.use.tcp.nodelay";

    public static final String CONCURRENT_LOOKUP_REQUESTS = "pulsar.concurrent.lookup.requests";
    public static final String MAX_NUMBER_OF_REJECTED_REQUESTS_PER_CONNECTION = "pulsar.max.number.rejected.request.per.connection";

    public static final String KEEPALIVE_INTERVAL_MS = "pulsar.keepalive.interval.ms";

    public static ClientBuilder getClientBuilder(Properties properties) {
        ClientBuilder clientBuilder = PulsarClient.builder();

        if (properties.containsKey(AUTHENTICATION_CLASS)) {
            String className = properties.getProperty(AUTHENTICATION_CLASS);
            try {
                if (properties.containsKey(AUTHENTICATION_PARAMS_STRING)) {
                    String authParamsString = (String) properties.get(AUTHENTICATION_PARAMS_STRING);
                    clientBuilder.authentication(className, authParamsString);
                } else if (properties.containsKey(AUTHENTICATION_PARAMS_MAP)) {
                    Map<String, String> authParams = (Map<String, String>) properties.get(AUTHENTICATION_PARAMS_MAP);
                    clientBuilder.authentication(className, authParams);
                } else {
                    @SuppressWarnings("unchecked")
                    Class<Authentication> clazz = (Class<Authentication>) Class.forName(className);
                    Authentication auth = clazz.newInstance();
                    clientBuilder.authentication(auth);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        clientBuilder.enableTls(Boolean.parseBoolean(properties.getProperty(USE_TLS, "false")));
        clientBuilder.allowTlsInsecureConnection(
                Boolean.parseBoolean(properties.getProperty(TLS_ALLOW_INSECURE_CONNECTION, "false")));
        clientBuilder.enableTlsHostnameVerification(
                Boolean.parseBoolean(properties.getProperty(TLS_HOSTNAME_VERIFICATION, "false")));

        if (properties.containsKey(TLS_TRUST_CERTS_FILE_PATH)) {
            clientBuilder.tlsTrustCertsFilePath(properties.getProperty(TLS_TRUST_CERTS_FILE_PATH));
        }

        if (properties.containsKey(OPERATION_TIMEOUT_MS)) {
            clientBuilder.operationTimeout(Integer.parseInt(properties.getProperty(OPERATION_TIMEOUT_MS)),
                    TimeUnit.MILLISECONDS);
        }

        if (properties.containsKey(STATS_INTERVAL_SECONDS)) {
            clientBuilder.statsInterval(Integer.parseInt(properties.getProperty(STATS_INTERVAL_SECONDS)),
                    TimeUnit.SECONDS);
        }

        if (properties.containsKey(NUM_IO_THREADS)) {
            clientBuilder.ioThreads(Integer.parseInt(properties.getProperty(NUM_IO_THREADS)));
        }

        if (properties.containsKey(CONNECTIONS_PER_BROKER)) {
            clientBuilder.connectionsPerBroker(Integer.parseInt(properties.getProperty(CONNECTIONS_PER_BROKER)));
        }

        if (properties.containsKey(USE_TCP_NODELAY)) {
            clientBuilder.enableTcpNoDelay(Boolean.parseBoolean(properties.getProperty(USE_TCP_NODELAY)));
        }

        if (properties.containsKey(CONCURRENT_LOOKUP_REQUESTS)) {
            clientBuilder.maxConcurrentLookupRequests(Integer.parseInt(properties.getProperty(CONCURRENT_LOOKUP_REQUESTS)));
        }

        if (properties.containsKey(MAX_NUMBER_OF_REJECTED_REQUESTS_PER_CONNECTION)) {
            clientBuilder.maxNumberOfRejectedRequestPerConnection(
                    Integer.parseInt(properties.getProperty(MAX_NUMBER_OF_REJECTED_REQUESTS_PER_CONNECTION)));
        }

        if (properties.containsKey(KEEPALIVE_INTERVAL_MS)) {
            clientBuilder.keepAliveInterval(Integer.parseInt(properties.getProperty(KEEPALIVE_INTERVAL_MS)),
                    TimeUnit.MILLISECONDS);
        }

        return clientBuilder;
    }
}
