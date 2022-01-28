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

import static com.google.common.base.Preconditions.checkArgument;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

/**
 * Class used to specify client side configuration like authentication, etc..
 *
 * @deprecated Use {@link PulsarClient#builder()} to construct and configure a new {@link PulsarClient} instance
 */
@Deprecated
public class ClientConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ClientConfigurationData confData = new ClientConfigurationData();

    /**
     * @return the authentication provider to be used
     */
    public Authentication getAuthentication() {
        return confData.getAuthentication();
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration confData = new ClientConfiguration();
     * String authPluginClassName = "org.apache.pulsar.client.impl.auth.MyAuthentication";
     * String authParamsString = "key1:val1,key2:val2";
     * Authentication auth = AuthenticationFactory.create(authPluginClassName, authParamsString);
     * confData.setAuthentication(auth);
     * PulsarClient client = PulsarClient.create(serviceUrl, confData);
     * ....
     * </code>
     * </pre>
     *
     * @param authentication
     */
    public void setAuthentication(Authentication authentication) {
        confData.setAuthentication(authentication);
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration confData = new ClientConfiguration();
     * String authPluginClassName = "org.apache.pulsar.client.impl.auth.MyAuthentication";
     * String authParamsString = "key1:val1,key2:val2";
     * confData.setAuthentication(authPluginClassName, authParamsString);
     * PulsarClient client = PulsarClient.create(serviceUrl, confData);
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
    public void setAuthentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        confData.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParamsString));
    }

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p>
     * Example:
     * <p>
     *
     * <pre>
     * <code>
     * ClientConfiguration confData = new ClientConfiguration();
     * String authPluginClassName = "org.apache.pulsar.client.impl.auth.MyAuthentication";
     * Map<String, String> authParams = new HashMap<String, String>();
     * authParams.put("key1", "val1");
     * confData.setAuthentication(authPluginClassName, authParams);
     * PulsarClient client = PulsarClient.create(serviceUrl, confData);
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    public void setAuthentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        confData.setAuthentication(AuthenticationFactory.create(authPluginClassName, authParams));
    }

    /**
     * @return the operation timeout in ms
     */
    public long getOperationTimeoutMs() {
        return confData.getOperationTimeoutMs();
    }

    /**
     * Set the operation timeout <i>(default: 30 seconds)</i>.
     * <p>
     * Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
     * operation will be marked as failed
     *
     * @param operationTimeout
     *            operation timeout
     * @param unit
     *            time unit for {@code operationTimeout}
     */
    public void setOperationTimeout(int operationTimeout, TimeUnit unit) {
        checkArgument(operationTimeout >= 0);
        confData.setOperationTimeoutMs(unit.toMillis(operationTimeout));
    }

    /**
     * @return the number of threads to use for handling connections
     */
    public int getIoThreads() {
        return confData.getNumIoThreads();
    }

    /**
     * Set the number of threads to be used for handling connections to brokers <i>(default: 1 thread)</i>.
     *
     * @param numIoThreads
     */
    public void setIoThreads(int numIoThreads) {
        checkArgument(numIoThreads > 0);
        confData.setNumIoThreads(numIoThreads);
    }

    /**
     * @return the number of threads to use for message listeners
     */
    public int getListenerThreads() {
        return confData.getNumListenerThreads();
    }

    /**
     * Set the number of threads to be used for message listeners <i>(default: 1 thread)</i>.
     *
     * @param numListenerThreads
     */
    public void setListenerThreads(int numListenerThreads) {
        checkArgument(numListenerThreads > 0);
        confData.setNumListenerThreads(numListenerThreads);
    }

    /**
     * @return the max number of connections per single broker
     */
    public int getConnectionsPerBroker() {
        return confData.getConnectionsPerBroker();
    }

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
    public void setConnectionsPerBroker(int connectionsPerBroker) {
        checkArgument(connectionsPerBroker > 0, "Connections per broker need to be greater than 0");
        confData.setConnectionsPerBroker(connectionsPerBroker);
    }

    /**
     * @return whether TCP no-delay should be set on the connections
     */
    public boolean isUseTcpNoDelay() {
        return confData.isUseTcpNoDelay();
    }

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     * <p>
     * No-delay features make sure packets are sent out on the network as soon as possible, and it's critical to achieve
     * low latency publishes. On the other hand, sending out a huge number of small packets might limit the overall
     * throughput, so if latency is not a concern, it's advisable to set the <code>useTcpNoDelay</code> flag to false.
     * <p>
     * Default value is true
     *
     * @param useTcpNoDelay
     */
    public void setUseTcpNoDelay(boolean useTcpNoDelay) {
        confData.setUseTcpNoDelay(useTcpNoDelay);
    }

    /**
     * @return whether TLS encryption is used on the connection
     */
    public boolean isUseTls() {
        return confData.isUseTls();
    }

    /**
     * Configure whether to use TLS encryption on the connection <i>(default: false)</i>.
     *
     * @param useTls
     */
    public void setUseTls(boolean useTls) {
        confData.setUseTls(useTls);
    }

    /**
     * @return path to the trusted TLS certificate file
     */
    public String getTlsTrustCertsFilePath() {
        return confData.getTlsTrustCertsFilePath();
    }

    /**
     * Set the path to the trusted TLS certificate file.
     *
     * @param tlsTrustCertsFilePath
     */
    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        confData.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);
    }

    /**
     * @return whether the Pulsar client accept untrusted TLS certificate from broker
     */
    public boolean isTlsAllowInsecureConnection() {
        return confData.isTlsAllowInsecureConnection();
    }

    /**
     * Configure whether the Pulsar client accept untrusted TLS certificate from broker <i>(default: false)</i>.
     *
     * @param tlsAllowInsecureConnection
     */
    public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
        confData.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
    }

    /**
     * Stats will be activated with positive statsIntervalSeconds.
     *
     * @return the interval between each stat info <i>(default: 60 seconds)</i>
     */
    public long getStatsIntervalSeconds() {
        return confData.getStatsIntervalSeconds();
    }

    /**
     * Set the interval between each stat info <i>(default: 60 seconds)</i> Stats will be activated with positive.
     * statsIntervalSeconds It should be set to at least 1 second
     *
     * @param statsInterval
     *            the interval between each stat info
     * @param unit
     *            time unit for {@code statsInterval}
     */
    public void setStatsInterval(long statsInterval, TimeUnit unit) {
        confData.setStatsIntervalSeconds(unit.toSeconds(statsInterval));
    }

    /**
     * Get configured total allowed concurrent lookup-request.
     *
     * @return
     */
    public int getConcurrentLookupRequest() {
        return confData.getConcurrentLookupRequest();
    }

    /**
     * Number of concurrent lookup-requests allowed on each broker-connection to prevent overload on broker.
     * <i>(default: 50000)</i> It should be configured with higher value only in case of it requires to
     * produce/subscribe on thousands of topic using created {@link PulsarClient}
     *
     * @param concurrentLookupRequest
     */
    public void setConcurrentLookupRequest(int concurrentLookupRequest) {
        confData.setConcurrentLookupRequest(concurrentLookupRequest);
    }

    /**
     * Get configured max number of reject-request in a time-frame (30 seconds) after which connection will be closed.
     *
     * @return
     */
    public int getMaxNumberOfRejectedRequestPerConnection() {
        return confData.getMaxNumberOfRejectedRequestPerConnection();
    }

    /**
     * Set max number of broker-rejected requests in a certain time-frame (30 seconds) after which current connection.
     * will be closed and client creates a new connection that give chance to connect a different broker <i>(default:
     * 50)</i>
     *
     * @param maxNumberOfRejectedRequestPerConnection
     */
    public void setMaxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
        confData.setMaxNumberOfRejectedRequestPerConnection(maxNumberOfRejectedRequestPerConnection);
    }

    public boolean isTlsHostnameVerificationEnable() {
        return confData.isTlsHostnameVerificationEnable();
    }

    /**
     * It allows to validate hostname verification when client connects to broker over tls. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">rfc2818</a>
     *
     * @param tlsHostnameVerificationEnable
     */
    public void setTlsHostnameVerificationEnable(boolean tlsHostnameVerificationEnable) {
        confData.setTlsHostnameVerificationEnable(tlsHostnameVerificationEnable);
    }

    public ClientConfiguration setServiceUrl(String serviceUrl) {
        confData.setServiceUrl(serviceUrl);
        return this;
    }

    /**
     * Set the duration of time to wait for a connection to a broker to be established. If the duration
     * passes without a response from the broker, the connection attempt is dropped.
     *
     * @param duration the duration to wait
     * @param unit the time unit in which the duration is defined
     */
    public void setConnectionTimeout(int duration, TimeUnit unit) {
        confData.setConnectionTimeoutMs((int) unit.toMillis(duration));
    }

    /**
     * Get the duration of time for which the client will wait for a connection to a broker to be
     * established before giving up.
     *
     * @return the duration, in milliseconds
     */
    public long getConnectionTimeoutMs() {
        return confData.getConnectionTimeoutMs();
    }

    public ClientConfigurationData getConfigurationData() {
        return confData;
    }

}
