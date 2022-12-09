/*
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
package org.apache.pulsar.compaction;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.BookKeeperClientFactoryImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.internal.PropertiesUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactorTool {

    private static class Arguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile = "conf/broker.conf";

        @Parameter(names = {"-t", "--topic"}, description = "Topic to compact", required = true)
        private String topic;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static PulsarClient createClient(ServiceConfiguration brokerConfig) throws PulsarClientException {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(0, SizeUnit.BYTES);

        // Apply all arbitrary configuration. This must be called before setting any fields annotated as
        // @Secret on the ClientConfigurationData object because of the way they are serialized.
        // See https://github.com/apache/pulsar/issues/8509 for more information.
        clientBuilder.loadConf(PropertiesUtils.filterAndMapProperties(brokerConfig.getProperties(), "brokerClient_"));

        if (isNotBlank(brokerConfig.getBrokerClientAuthenticationPlugin())) {
            clientBuilder.authentication(brokerConfig.getBrokerClientAuthenticationPlugin(),
                    brokerConfig.getBrokerClientAuthenticationParameters());
        }

        AdvertisedListener internalListener = ServiceConfigurationUtils.getInternalListener(brokerConfig, "pulsar+ssl");
        if (internalListener.getBrokerServiceUrlTls() != null && brokerConfig.isBrokerClientTlsEnabled()) {
            clientBuilder.serviceUrl(internalListener.getBrokerServiceUrlTls().toString())
                    .allowTlsInsecureConnection(brokerConfig.isTlsAllowInsecureConnection());
            if (brokerConfig.isBrokerClientTlsEnabledWithKeyStore()) {
                clientBuilder.useKeyStoreTls(true)
                        .tlsKeyStoreType(brokerConfig.getBrokerClientTlsKeyStoreType())
                        .tlsKeyStorePath(brokerConfig.getBrokerClientTlsKeyStore())
                        .tlsKeyStorePassword(brokerConfig.getBrokerClientTlsKeyStorePassword())
                        .tlsTrustStoreType(brokerConfig.getBrokerClientTlsTrustStoreType())
                        .tlsTrustStorePath(brokerConfig.getBrokerClientTlsTrustStore())
                        .tlsTrustStorePassword(brokerConfig.getBrokerClientTlsTrustStorePassword());
            } else {
                clientBuilder.tlsTrustCertsFilePath(brokerConfig.getBrokerClientTrustCertsFilePath())
                        .tlsKeyFilePath(brokerConfig.getBrokerClientKeyFilePath())
                        .tlsCertificateFilePath(brokerConfig.getBrokerClientCertificateFilePath());
            }
        } else {
            internalListener = ServiceConfigurationUtils.getInternalListener(brokerConfig, "pulsar");
            clientBuilder.serviceUrl(internalListener.getBrokerServiceUrl().toString());
        }

        return clientBuilder.build();
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander(arguments);
        jcommander.setProgramName("PulsarTopicCompactor");

        // parse args by JCommander
        jcommander.parse(args);
        if (arguments.help) {
            jcommander.usage();
            System.exit(0);
        }

        if (arguments.generateDocs) {
            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("compact-topic", arguments);
            cmd.run(null);
            System.exit(0);
        }

        // init broker config
        if (isBlank(arguments.brokerConfigFile)) {
            jcommander.usage();
            throw new IllegalArgumentException("Need to specify a configuration file for broker");
        }

        final String filepath = Path.of(arguments.brokerConfigFile).toAbsolutePath().normalize().toString();
        log.info(String.format("read configuration file %s", filepath));
        final ServiceConfiguration brokerConfig =
                PulsarConfigurationLoader.create(filepath, ServiceConfiguration.class);


        if (isBlank(brokerConfig.getMetadataStoreUrl())) {
            final String message = String.format("""
                    Need to specify `metadataStoreUrl` or `zookeeperServers` in configuration file
                    or specify configuration file path from command line.
                    now configuration file path is=[%s]
                    """, filepath);
            throw new IllegalArgumentException(message);
        }

        @Cleanup(value = "shutdownNow")
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());

        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(brokerConfig.getMetadataStoreUrl(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis((int) brokerConfig.getMetadataStoreSessionTimeoutMillis())
                        .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                        .build());

        @Cleanup
        BookKeeperClientFactory bkClientFactory = new BookKeeperClientFactoryImpl();

        @Cleanup(value = "shutdownGracefully")
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("compactor-io"));

        @Cleanup
        BookKeeper bk = bkClientFactory.create(brokerConfig, store, eventLoopGroup, Optional.empty(), null);

        @Cleanup
        PulsarClient pulsar = createClient(brokerConfig);

        Compactor compactor = new TwoPhaseCompactor(brokerConfig, pulsar, bk, scheduler);
        long ledgerId = compactor.compact(arguments.topic).get();
        log.info("Compaction of topic {} complete. Compacted to ledger {}", arguments.topic, ledgerId);
    }

    private static final Logger log = LoggerFactory.getLogger(CompactorTool.class);
}
