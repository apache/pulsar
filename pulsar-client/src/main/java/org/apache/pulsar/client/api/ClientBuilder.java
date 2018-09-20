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
package org.apache.pulsar.client.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

/**
 * Builder interface that is used to construct a {@link PulsarClient} instance.
 *
 * @since 2.0.0
 */
public interface ClientBuilder extends Cloneable {

    /**
     * @return the new {@link PulsarClient} instance
     */
    PulsarClient build() throws PulsarClientException;

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     * <p>Example:
     * <pre>
     * Map&lt;String, Object&gt; config = new HashMap&lt;&gt;();
     * config.put("serviceUrl", "pulsar://localhost:5550");
     * config.put("numIoThreads", 20);
     *
     * ClientBuilder builder = ...;
     * builder = builder.loadConf(config);
     *
     * PulsarClient client = builder.build();
     * </pre>
     *
     * @param config configuration to load
     * @return client builder instance
     */
    ClientBuilder loadConf(Map<String, Object> config);

    /**
     * Create a copy of the current client builder.
     * <p>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * ClientBuilder builder = PulsarClient.builder().ioThreads(8).listenerThreads(4);
     *
     * PulsarClient client1 = builder.clone().serviceUrl(URL_1).build();
     * PulsarClient client2 = builder.clone().serviceUrl(URL_2).build();
     * </pre>
     */
    ClientBuilder clone();

    /**
     * Configure the service URL for the Pulsar service.
     * <p>
     * This parameter is required
     *
     * @param serviceUrl
     * @return
     */
    ClientBuilder serviceUrl(String serviceUrl);

    /**
     * Configure the service URL provider for Pulsar service
     * @param serviceUrlProvider
     * @return
     */
    ClientBuilder serviceUrlProvider(ServiceUrlProvider serviceUrlProvider);

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * Authentication auth = AuthenticationFactor.create(AUTH_CLASS, conf);
     *
     * PulsarClient client = PulsarClient.builder()
     *          .serviceUrl(SERVICE_URL)
     *          .authentication(auth)
     *          .build();
     * ....
     * </code>
     * </pre>
     *
     * @param authentication
     *            an instance of the {@link Authentication} provider already constructed
     */
    ClientBuilder authentication(Authentication authentication);

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     * String AUTH_PARAMS = "tlsCertFile:/my/cert/file,tlsKeyFile:/my/key/file";
     *
     * PulsarClient client = PulsarClient.builder()
     *          .serviceUrl(SERVICE_URL)
     *          .authentication(AUTH_CLASS, AUTH_PARAMS)
     *          .build();
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    ClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * PulsarClient client = PulsarClient.builder()
     *          .serviceUrl(SERVICE_URL)
     *          .authentication(AUTH_CLASS, conf)
     *          .build();
     * ....
     * </code>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    ClientBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException;

    /**
     * Set the operation timeout <i>(default: 30 seconds)</i>
     * <p>
     * Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
     * operation will be marked as failed
     *
     * @param operationTimeout
     *            operation timeout
     * @param unit
     *            time unit for {@code operationTimeout}
     */
    ClientBuilder operationTimeout(int operationTimeout, TimeUnit unit);

    /**
     * Set the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>
     *
     * @param numIoThreads
     */
    ClientBuilder ioThreads(int numIoThreads);

    /**
     * Set the number of threads to be used for message listeners <i>(default: 1 thread)</i>
     *
     * @param numListenerThreads
     */
    ClientBuilder listenerThreads(int numListenerThreads);

    /**
     * Sets the max number of connection that the client library will open to a single broker.
     * <p>
     * By default, the connection pool will use a single connection for all the producers and consumers. Increasing this
     * parameter may improve throughput when using many producers over a high latency connection.
     * <p>
     *
     * @param connectionsPerBroker
     *            max number of connections per broker (needs to be greater than 0)
     */
    ClientBuilder connectionsPerBroker(int connectionsPerBroker);

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     * <p>
     * No-delay features make sure packets are sent out on the network as soon as possible, and it's critical to achieve
     * low latency publishes. On the other hand, sending out a huge number of small packets might limit the overall
     * throughput, so if latency is not a concern, it's advisable to set the <code>useTcpNoDelay</code> flag to false.
     * <p>
     * Default value is true
     *
     * @param enableTcpNoDelay
     */
    ClientBuilder enableTcpNoDelay(boolean enableTcpNoDelay);

    /**
     * Configure whether to use TLS encryption on the connection
     * <i>(default: true if serviceUrl starts with "pulsar+ssl://", false otherwise)</i>
     *
     * @param enableTls
     * @deprecated use "pulsar+ssl://" in serviceUrl to enable
     */
    @Deprecated
    ClientBuilder enableTls(boolean enableTls);

    /**
     * Set the path to the trusted TLS certificate file
     *
     * @param tlsTrustCertsFilePath
     */
    ClientBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath);

    /**
     * Configure whether the Pulsar client accept untrusted TLS certificate from broker <i>(default: false)</i>
     *
     * @param allowTlsInsecureConnection
     */
    ClientBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection);

    /**
     * It allows to validate hostname verification when client connects to broker over tls. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">rfc2818</a>
     *
     * @param enableTlsHostnameVerification
     */
    ClientBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification);

    /**
     * Set the interval between each stat info <i>(default: 60 seconds)</i> Stats will be activated with positive
     * statsIntervalSeconds It should be set to at least 1 second
     *
     * @param statsIntervalSeconds
     *            the interval between each stat info
     * @param unit
     *            time unit for {@code statsInterval}
     */
    ClientBuilder statsInterval(long statsInterval, TimeUnit unit);

    /**
     * Number of concurrent lookup-requests allowed to send on each broker-connection to prevent overload on broker.
     * <i>(default: 5000)</i> It should be configured with higher value only in case of it requires to produce/subscribe
     * on thousands of topic using created {@link PulsarClient}
     *
     * @param maxConcurrentLookupRequests
     */
    ClientBuilder maxConcurrentLookupRequests(int maxConcurrentLookupRequests);

    /**
     * Number of max lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 50000)</i> It should be bigger than maxConcurrentLookupRequests.
     * Requests that inside maxConcurrentLookupRequests already send to broker, and requests beyond
     * maxConcurrentLookupRequests and under maxLookupRequests will wait in each client cnx.
     *
     * @param maxLookupRequests
     */
    ClientBuilder maxLookupRequests(int maxLookupRequests);

    /**
     * Set max number of broker-rejected requests in a certain time-frame (30 seconds) after which current connection
     * will be closed and client creates a new connection that give chance to connect a different broker <i>(default:
     * 50)</i>
     *
     * @param maxNumberOfRejectedRequestPerConnection
     */
    ClientBuilder maxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection);

    /**
     * Set keep alive interval in seconds for each client-broker-connection. <i>(default: 30)</i>.
     *
     * @param keepAliveIntervalSeconds
     * @param unit time unit for {@code statsInterval}
     */
    ClientBuilder keepAliveInterval(int keepAliveIntervalSeconds, TimeUnit unit);
}
