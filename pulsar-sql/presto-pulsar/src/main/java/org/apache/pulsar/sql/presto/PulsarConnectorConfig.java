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
package org.apache.pulsar.sql.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.configuration.Config;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import javax.validation.constraints.NotNull;
import javax.ws.rs.client.ClientBuilder;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.protocol.Commands;

/**
 * This object handles configuration of the Pulsar connector for the Presto engine.
 */
public class PulsarConnectorConfig implements AutoCloseable {

    private String brokerServiceUrl = "http://localhost:8080";
    private String webServiceUrl = ""; //leave empty
    private String zookeeperUri = "localhost:2181";
    private int entryReadBatchSize = 100;
    private int targetNumSplits = 2;
    private int maxSplitMessageQueueSize = 10000;
    private int maxSplitEntryQueueSize = 1000;
    private long maxSplitQueueSizeBytes = -1;
    private int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;
    private String statsProvider = NullStatsProvider.class.getName();

    private Map<String, String> statsProviderConfigs = new HashMap<>();
    private String authPluginClassName;
    private String authParams;
    private String tlsTrustCertsFilePath;
    private Boolean tlsAllowInsecureConnection;
    private Boolean tlsHostnameVerificationEnable;

    private boolean namespaceDelimiterRewriteEnable = false;
    private String rewriteNamespaceDelimiter = "/";

    // --- Ledger Offloading ---
    private String managedLedgerOffloadDriver = null;
    private int managedLedgerOffloadMaxThreads = 2;
    private String offloadersDirectory = "./offloaders";
    private Map<String, String> offloaderProperties = new HashMap<>();

    private PulsarAdmin pulsarAdmin;

    // --- Bookkeeper
    private int bookkeeperThrottleValue = 0;
    private int bookkeeperNumIOThreads = 2 * Runtime.getRuntime().availableProcessors();
    private int bookkeeperNumWorkerThreads = Runtime.getRuntime().availableProcessors();
    private boolean bookkeeperUseV2Protocol = true;
    private int bookkeeperExplicitInterval = 0;

    // --- ManagedLedger
    private long managedLedgerCacheSizeMB = 0L;
    private int managedLedgerNumWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int managedLedgerNumSchedulerThreads = Runtime.getRuntime().availableProcessors();

    // --- Nar extraction
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @NotNull
    public String getBrokerServiceUrl() {
        if (StringUtils.isEmpty(webServiceUrl)){
            return brokerServiceUrl;
        } else {
            return getWebServiceUrl();
        }
    }
    @Config("pulsar.broker-service-url")
    public PulsarConnectorConfig setBrokerServiceUrl(String brokerServiceUrl) {
        this.brokerServiceUrl = brokerServiceUrl;
        return this;
    }
    @Config("pulsar.web-service-url")
    public PulsarConnectorConfig setWebServiceUrl(String webServiceUrl) {
        this.webServiceUrl = webServiceUrl;
        return this;
    }

    public String getWebServiceUrl() {
        return webServiceUrl;
    }

    @Config("pulsar.max-message-size")
    public PulsarConnectorConfig setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public int getMaxMessageSize() {
        return this.maxMessageSize;
    }

    @NotNull
    public String getZookeeperUri() {
        return this.zookeeperUri;
    }

    @Config("pulsar.zookeeper-uri")
    public PulsarConnectorConfig setZookeeperUri(String zookeeperUri) {
        this.zookeeperUri = zookeeperUri;
        return this;
    }

    @NotNull
    public int getMaxEntryReadBatchSize() {
        return this.entryReadBatchSize;
    }

    @Config("pulsar.max-entry-read-batch-size")
    public PulsarConnectorConfig setMaxEntryReadBatchSize(int batchSize) {
        this.entryReadBatchSize = batchSize;
        return this;
    }

    @NotNull
    public int getTargetNumSplits() {
        return this.targetNumSplits;
    }

    @Config("pulsar.target-num-splits")
    public PulsarConnectorConfig setTargetNumSplits(int targetNumSplits) {
        this.targetNumSplits = targetNumSplits;
        return this;
    }

    @NotNull
    public int getMaxSplitMessageQueueSize() {
        return this.maxSplitMessageQueueSize;
    }

    @Config("pulsar.max-split-message-queue-size")
    public PulsarConnectorConfig setMaxSplitMessageQueueSize(int maxSplitMessageQueueSize) {
        this.maxSplitMessageQueueSize = maxSplitMessageQueueSize;
        return this;
    }

    @NotNull
    public int getMaxSplitEntryQueueSize() {
        return this.maxSplitEntryQueueSize;
    }

    @Config("pulsar.max-split-entry-queue-size")
    public PulsarConnectorConfig setMaxSplitEntryQueueSize(int maxSplitEntryQueueSize) {
        this.maxSplitEntryQueueSize = maxSplitEntryQueueSize;
        return this;
    }

    @NotNull
    public long getMaxSplitQueueSizeBytes() {
        return this.maxSplitQueueSizeBytes;
    }

    @Config("pulsar.max-split-queue-cache-size")
    public PulsarConnectorConfig setMaxSplitQueueSizeBytes(long maxSplitQueueSizeBytes) {
        this.maxSplitQueueSizeBytes = maxSplitQueueSizeBytes;
        return this;
    }

    @NotNull
    public String getStatsProvider() {
        return statsProvider;
    }

    @Config("pulsar.stats-provider")
    public PulsarConnectorConfig setStatsProvider(String statsProvider) {
        this.statsProvider = statsProvider;
        return this;
    }

    @NotNull
    public Map<String, String> getStatsProviderConfigs() {
        return statsProviderConfigs;
    }

    @Config("pulsar.stats-provider-configs")
    public PulsarConnectorConfig setStatsProviderConfigs(String statsProviderConfigs) throws IOException {
        this.statsProviderConfigs = new ObjectMapper().readValue(statsProviderConfigs, Map.class);
        return this;
    }

    public String getRewriteNamespaceDelimiter() {
        return rewriteNamespaceDelimiter;
    }

    @Config("pulsar.rewrite-namespace-delimiter")
    public PulsarConnectorConfig setRewriteNamespaceDelimiter(String rewriteNamespaceDelimiter) {
        Matcher m = NamedEntity.NAMED_ENTITY_PATTERN.matcher(rewriteNamespaceDelimiter);
        if (m.matches()) {
            throw new IllegalArgumentException(
                    "Can't use " + rewriteNamespaceDelimiter + "as delimiter, "
                            + "because delimiter must contain characters which name of namespace not allowed"
            );
        }
        this.rewriteNamespaceDelimiter = rewriteNamespaceDelimiter;
        return this;
    }

    public boolean getNamespaceDelimiterRewriteEnable() {
        return namespaceDelimiterRewriteEnable;
    }

    @Config("pulsar.namespace-delimiter-rewrite-enable")
    public PulsarConnectorConfig setNamespaceDelimiterRewriteEnable(boolean namespaceDelimiterRewriteEnable) {
        this.namespaceDelimiterRewriteEnable = namespaceDelimiterRewriteEnable;
        return this;
    }

    // --- Ledger Offloading ---

    public int getManagedLedgerOffloadMaxThreads() {
        return this.managedLedgerOffloadMaxThreads;
    }

    @Config("pulsar.managed-ledger-offload-max-threads")
    public PulsarConnectorConfig setManagedLedgerOffloadMaxThreads(int managedLedgerOffloadMaxThreads)
        throws IOException {
        this.managedLedgerOffloadMaxThreads = managedLedgerOffloadMaxThreads;
        return this;
    }

    public String getManagedLedgerOffloadDriver() {
        return this.managedLedgerOffloadDriver;
    }

    @Config("pulsar.managed-ledger-offload-driver")
    public PulsarConnectorConfig setManagedLedgerOffloadDriver(String managedLedgerOffloadDriver) throws IOException {
        this.managedLedgerOffloadDriver = managedLedgerOffloadDriver;
        return this;
    }

    public String getOffloadersDirectory() {
        return this.offloadersDirectory;
    }


    @Config("pulsar.offloaders-directory")
    public PulsarConnectorConfig setOffloadersDirectory(String offloadersDirectory) throws IOException {
        this.offloadersDirectory = offloadersDirectory;
        return this;
    }

    public Map<String, String> getOffloaderProperties() {
        return this.offloaderProperties;
    }

    @Config("pulsar.offloader-properties")
    public PulsarConnectorConfig setOffloaderProperties(String offloaderProperties) throws IOException {
        this.offloaderProperties = new ObjectMapper().readValue(offloaderProperties, Map.class);
        return this;
    }

    // --- Authentication ---

    public String getAuthPlugin() {
        return this.authPluginClassName;
    }

    @Config("pulsar.auth-plugin")
    public PulsarConnectorConfig setAuthPlugin(String authPluginClassName) throws IOException {
        this.authPluginClassName = authPluginClassName;
        return this;
    }

    public String getAuthParams() {
        return this.authParams;
    }

    @Config("pulsar.auth-params")
    public PulsarConnectorConfig setAuthParams(String authParams) throws IOException {
        this.authParams = authParams;
        return this;
    }

    public Boolean isTlsAllowInsecureConnection() {
        return tlsAllowInsecureConnection;
    }

    @Config("pulsar.tls-allow-insecure-connection")
    public PulsarConnectorConfig setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
        this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
        return this;
    }

    public Boolean isTlsHostnameVerificationEnable() {
        return tlsHostnameVerificationEnable;
    }

    @Config("pulsar.tls-hostname-verification-enable")
    public PulsarConnectorConfig setTlsHostnameVerificationEnable(boolean tlsHostnameVerificationEnable) {
        this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
        return this;
    }

    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    @Config("pulsar.tls-trust-cert-file-path")
    public PulsarConnectorConfig setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
        return this;
    }

    // --- Bookkeeper Config ---

    public int getBookkeeperThrottleValue() {
        return bookkeeperThrottleValue;
    }

    @Config("pulsar.bookkeeper-throttle-value")
    public PulsarConnectorConfig setBookkeeperThrottleValue(int bookkeeperThrottleValue) {
        this.bookkeeperThrottleValue = bookkeeperThrottleValue;
        return this;
    }

    public int getBookkeeperNumIOThreads() {
        return bookkeeperNumIOThreads;
    }

    @Config("pulsar.bookkeeper-num-io-threads")
    public PulsarConnectorConfig setBookkeeperNumIOThreads(int bookkeeperNumIOThreads) {
        this.bookkeeperNumIOThreads = bookkeeperNumIOThreads;
        return this;
    }

    public int getBookkeeperNumWorkerThreads() {
        return bookkeeperNumWorkerThreads;
    }

    @Config("pulsar.bookkeeper-num-worker-threads")
    public PulsarConnectorConfig setBookkeeperNumWorkerThreads(int bookkeeperNumWorkerThreads) {
        this.bookkeeperNumWorkerThreads = bookkeeperNumWorkerThreads;
        return this;
    }

    public boolean getBookkeeperUseV2Protocol() {
        return bookkeeperUseV2Protocol;
    }

    @Config("pulsar.bookkeeper-use-v2-protocol")
    public PulsarConnectorConfig setBookkeeperUseV2Protocol(boolean bookkeeperUseV2Protocol) {
        this.bookkeeperUseV2Protocol = bookkeeperUseV2Protocol;
        return this;
    }

    public int getBookkeeperExplicitInterval() {
        return bookkeeperExplicitInterval;
    }

    @Config("pulsar.bookkeeper-explicit-interval")
    public PulsarConnectorConfig setBookkeeperExplicitInterval(int bookkeeperExplicitInterval) {
        this.bookkeeperExplicitInterval = bookkeeperExplicitInterval;
        return this;
    }

    // --- ManagedLedger
    public long getManagedLedgerCacheSizeMB() {
        return managedLedgerCacheSizeMB;
    }

    @Config("pulsar.managed-ledger-cache-size-MB")
    public PulsarConnectorConfig setManagedLedgerCacheSizeMB(int managedLedgerCacheSizeMB) {
        this.managedLedgerCacheSizeMB = managedLedgerCacheSizeMB * 1024 * 1024;
        return this;
    }

    public int getManagedLedgerNumWorkerThreads() {
        return managedLedgerNumWorkerThreads;
    }

    @Config("pulsar.managed-ledger-num-worker-threads")
    public PulsarConnectorConfig setManagedLedgerNumWorkerThreads(int managedLedgerNumWorkerThreads) {
        this.managedLedgerNumWorkerThreads = managedLedgerNumWorkerThreads;
        return this;
    }

    public int getManagedLedgerNumSchedulerThreads() {
        return managedLedgerNumSchedulerThreads;
    }

    @Config("pulsar.managed-ledger-num-scheduler-threads")
    public PulsarConnectorConfig setManagedLedgerNumSchedulerThreads(int managedLedgerNumSchedulerThreads) {
        this.managedLedgerNumSchedulerThreads = managedLedgerNumSchedulerThreads;
        return this;
    }

    // --- Nar extraction config
    public String getNarExtractionDirectory() {
        return narExtractionDirectory;
    }

    @Config("pulsar.nar-extraction-directory")
    public PulsarConnectorConfig setNarExtractionDirectory(String narExtractionDirectory) {
        this.narExtractionDirectory = narExtractionDirectory;
        return this;
    }

    @NotNull
    public PulsarAdmin getPulsarAdmin() throws PulsarClientException {
        if (this.pulsarAdmin == null) {
            PulsarAdminBuilder builder = PulsarAdmin.builder();

            if (getAuthPlugin() != null) {
                builder.authentication(getAuthPlugin(), getAuthParams());
            }

            if (isTlsAllowInsecureConnection() != null) {
                builder.allowTlsInsecureConnection(isTlsAllowInsecureConnection());
            }

            if (isTlsHostnameVerificationEnable() != null) {
                builder.enableTlsHostnameVerification(isTlsHostnameVerificationEnable());
            }

            if (getTlsTrustCertsFilePath() != null) {
                builder.tlsTrustCertsFilePath(getTlsTrustCertsFilePath());
            }

            builder.setContextClassLoader(ClientBuilder.class.getClassLoader());
            this.pulsarAdmin = builder.serviceHttpUrl(getBrokerServiceUrl()).build();
        }
        return this.pulsarAdmin;
    }

    public OffloadPoliciesImpl getOffloadPolices() {
        Properties offloadProperties = new Properties();
        offloadProperties.putAll(getOffloaderProperties());
        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create(offloadProperties);
        offloadPolicies.setManagedLedgerOffloadDriver(getManagedLedgerOffloadDriver());
        offloadPolicies.setManagedLedgerOffloadMaxThreads(getManagedLedgerOffloadMaxThreads());
        offloadPolicies.setOffloadersDirectory(getOffloadersDirectory());
        return offloadPolicies;
    }

    @Override
    public void close() throws Exception {
        this.pulsarAdmin.close();
    }

    @Override
    public String toString() {
       if (StringUtils.isEmpty(webServiceUrl)){
           return "PulsarConnectorConfig{"
            + "brokerServiceUrl='" + brokerServiceUrl + '\''
            + '}';
        } else {
            return "PulsarConnectorConfig{"
            + "brokerServiceUrl='" + webServiceUrl + '\''
            + '}';
        }
    }
}
