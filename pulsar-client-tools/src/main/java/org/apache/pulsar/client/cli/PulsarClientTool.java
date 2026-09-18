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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.internal.CommandHook;
import org.apache.pulsar.internal.CommanderFactory;
import org.apache.pulsar.tls.TlsPolicy;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Command(
        name = "pulsar-client",
        mixinStandardHelpOptions = true,
        versionProvider = PulsarVersionProvider.class,
        scope = ScopeType.INHERIT
)
public class PulsarClientTool implements CommandHook {

    private PulsarClientPropertiesProvider pulsarClientPropertiesProvider;

    @Getter
    @Command(description = "Produce or consume messages on a specified topic")
    public static class RootParams {
        @Option(names = {"--url"}, descriptionKey = "brokerServiceUrl",
                description = "Broker URL to which to connect.")
        String serviceURL = null;

        @Option(names = {"--proxy-url"}, descriptionKey = "proxyServiceUrl",
                description = "Proxy-server URL to which to connect.")
        String proxyServiceURL = null;

        @Option(names = {"--proxy-protocol"}, descriptionKey = "proxyProtocol",
                description = "Proxy protocol to select type of routing at proxy.",
                converter = ProxyProtocolConverter.class)
        ProxyProtocol proxyProtocol = null;

        @Option(names = {"--auth-plugin"}, descriptionKey = "authPlugin",
                description = "Authentication plugin class name.")
        String authPluginClassName = null;

        @Option(names = {"--listener-name"}, description = "Listener name for the broker.")
        String listenerName = null;

        @Option(
                names = {"--auth-params"},
                descriptionKey = "authParams",
                description = "Authentication parameters, whose format is determined by the implementation "
                        + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                        + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".")
        String authParams = null;

        @Option(names = {"--tlsTrustCertsFilePath"},
                descriptionKey = "tlsTrustCertsFilePath",
                description = "File path to client trust certificates")
        String tlsTrustCertsFilePath;

        @Option(names = {"-ml", "--memory-limit"}, description = "Configure the Pulsar client memory limit "
                + "(eg: 32M, 64M)", descriptionKey = "memoryLimit",
                converter = ByteUnitToLongConverter.class)
        long memoryLimit = 0L;
    }


    @ArgGroup(exclusive = false)
    protected RootParams rootParams = new RootParams();

    protected final CommandLine commander;
    protected CmdProduce produceCommand;
    protected CmdConsume consumeCommand;
    protected CmdRead readCommand;
    protected CmdProduceV4 produceV4Command;
    protected CmdConsumeV4 consumeV4Command;
    protected CmdReadV4 readV4Command;
    CmdGenerateDocumentation generateDocumentation;

    public PulsarClientTool(Properties properties) {
        // Use -v instead -V
        System.setProperty("picocli.version.name.0", "-v");
        commander = CommanderFactory.createRootCommanderWithHook(this, null);
        initCommander(properties);
    }

    @Override
    @SneakyThrows
    public int preRun() {
        return updateConfig();
    }

    protected void initCommander(Properties properties) {
        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();
        readCommand = new CmdRead();
        produceV4Command = new CmdProduceV4();
        consumeV4Command = new CmdConsumeV4();
        readV4Command = new CmdReadV4();
        generateDocumentation = new CmdGenerateDocumentation();

        pulsarClientPropertiesProvider = PulsarClientPropertiesProvider.create(properties);
        commander.setDefaultValueProvider(pulsarClientPropertiesProvider);
        commander.addSubcommand("produce", produceCommand);
        commander.addSubcommand("consume", consumeCommand);
        commander.addSubcommand("read", readCommand);
        // The same commands driven by the v4 (pulsar-client-original) client, for the capabilities
        // the V5 client cannot express: KeyValue schemas, non-durable subscriptions, a real topic
        // regex, --start-timestamp, the v4 Reader with a <ledgerId>:<entryId> start position, and
        // the client.conf keys that have no dedicated CLI flag.
        commander.addSubcommand("produce-v4", produceV4Command);
        commander.addSubcommand("consume-v4", consumeV4Command);
        commander.addSubcommand("read-v4", readV4Command);
        commander.addSubcommand("generate_documentation", generateDocumentation);
        enableCaseInsensitiveEnums();
    }

    /**
     * Accept enum flag values regardless of case across the root command and all subcommands. The
     * V5 client enums are uppercase (LATEST, EARLIEST, FAIL, ...) while users have long passed the
     * mixed-case v4 spellings (Latest, Earliest, ...); case-insensitive parsing keeps that flag UX
     * working. Picocli's {@code setCaseInsensitiveEnumValuesAllowed} does not propagate to
     * subcommands, so it must be applied to each command explicitly.
     */
    private void enableCaseInsensitiveEnums() {
        applyCaseInsensitiveEnums(commander);
    }

    private static void applyCaseInsensitiveEnums(CommandLine cmd) {
        cmd.setCaseInsensitiveEnumValuesAllowed(true);
        cmd.getSubcommands().values().forEach(PulsarClientTool::applyCaseInsensitiveEnums);
    }

    protected void addCommand(String name, Object cmd) {
        commander.addSubcommand(name, cmd);
        enableCaseInsensitiveEnums();
    }

    private int updateConfig() throws UnsupportedAuthenticationException {
        final Properties properties = pulsarClientPropertiesProvider.getProperties();

        PulsarClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(MemorySize.ofBytes(rootParams.memoryLimit));

        // The v4 Authentication object is still needed by the WebSocket produce/consume path,
        // which talks HTTP and is not migrated to the binary-only V5 client.
        Authentication authentication = null;
        if (isNotBlank(this.rootParams.authPluginClassName)) {
            authentication = AuthenticationFactory.create(rootParams.authPluginClassName, rootParams.authParams);
            try {
                clientBuilder.authentication(rootParams.authPluginClassName, rootParams.authParams);
            } catch (org.apache.pulsar.client.api.v5.PulsarClientException e) {
                throw new UnsupportedAuthenticationException(e);
            }
        }
        if (isNotBlank(this.rootParams.listenerName)) {
            clientBuilder.listenerName(this.rootParams.listenerName);
        }

        // serviceUrl is only set on the V5 (binary) client for pulsar:// / pulsar+ssl:// URLs.
        // A ws:// URL means the WebSocket path is used instead, which never builds a V5 client,
        // and the V5 builder rejects non-broker schemes at configure time.
        String serviceUrl = rootParams.serviceURL;
        if (serviceUrl != null
                && (serviceUrl.startsWith("pulsar://") || serviceUrl.startsWith("pulsar+ssl://"))) {
            clientBuilder.serviceUrl(serviceUrl);
        }

        applyTlsPolicy(clientBuilder, serviceUrl, properties);

        if (isNotBlank(rootParams.proxyServiceURL)) {
            if (rootParams.proxyProtocol == null) {
                commander.getErr().println("proxy-protocol must be provided with proxy-url");
                return 1;
            }
            clientBuilder.connectionPolicy(ConnectionPolicy.builder()
                    .proxy(rootParams.proxyServiceURL,
                            org.apache.pulsar.client.api.v5.config.ProxyProtocol.valueOf(
                                    rootParams.proxyProtocol.name()))
                    .build());
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.readCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);

        // Deliberately a supplier rather than a built builder: this method is the preRun() hook and
        // runs for every invocation, including `--help`, `generate_documentation` and the V5
        // commands. Building the v4 builder here would make all of them depend on a service URL and
        // on client.conf keys that only the v4 client parses. It is resolved when a *-v4 command
        // actually runs.
        final Authentication v4Authentication = authentication;
        Supplier<ClientBuilder> v4ClientBuilder = () -> buildV4ClientBuilder(properties, v4Authentication);
        this.produceV4Command.updateConfig(v4ClientBuilder, authentication, this.rootParams.serviceURL);
        this.consumeV4Command.updateConfig(v4ClientBuilder, authentication, this.rootParams.serviceURL);
        this.readV4Command.updateConfig(v4ClientBuilder, authentication, this.rootParams.serviceURL);
        return 0;
    }

    /**
     * Build the v4 client for the {@code *-v4} subcommands. Unlike the V5 builder this one keeps
     * {@code loadConf}, so every {@code client.conf} key still applies without a hand-written
     * translation, and it accepts {@code http://} / {@code https://} service URLs.
     */
    @VisibleForTesting
    ClientBuilder buildV4ClientBuilder(Properties properties, Authentication authentication) {
        Map<String, Object> conf = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            conf.put(key, properties.getProperty(key));
        }

        // Fully qualified: the unqualified PulsarClient in this file is the V5 one.
        ClientBuilder clientBuilder = org.apache.pulsar.client.api.PulsarClient.builder().loadConf(conf)
                .memoryLimit(rootParams.memoryLimit, SizeUnit.BYTES);
        if (authentication != null) {
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.rootParams.listenerName)) {
            clientBuilder.listenerName(this.rootParams.listenerName);
        }
        // Both are applied unconditionally on purpose. rootParams is itself populated from these
        // same properties (each option declares the client.conf key as its descriptionKey, and the
        // commander's default-value provider is a PropertiesDefaultProvider over them), so this
        // rewrites the value loadConf already applied, or applies the CLI override. serviceUrl is
        // additionally load-bearing: loadConf only reads the `serviceUrl` key, while client.conf
        // supplies `brokerServiceUrl` / `webServiceUrl`.
        clientBuilder.serviceUrl(rootParams.serviceURL);
        clientBuilder.tlsTrustCertsFilePath(this.rootParams.tlsTrustCertsFilePath);
        // A missing proxy protocol is already rejected by updateConfig(), the preRun() hook that
        // creates this supplier, and again by ClientBuilderImpl.proxyServiceUrl(). The blank check
        // stays because that same re-check rejects a null protocol: without a proxy configured at
        // all, calling through would throw rather than leave the proxy unset.
        if (isNotBlank(rootParams.proxyServiceURL)) {
            clientBuilder.proxyServiceUrl(rootParams.proxyServiceURL, rootParams.proxyProtocol);
        }
        return clientBuilder;
    }

    /**
     * Translate the client.conf TLS settings onto the typed V5 {@link TlsPolicy}. V5 has no
     * untyped {@code loadConf}, so the conf-file keys that have no dedicated CLI flag
     * ({@code tlsAllowInsecureConnection}, {@code tlsEnableHostnameVerification}, the mTLS
     * cert/key paths, the {@code useKeyStoreTls} keystore keys) are read from the properties here.
     *
     * <p>TLS is enabled only when the service URL uses {@code pulsar+ssl://} or the conf sets
     * {@code useTls=true}; otherwise we leave the policy untouched so a plaintext broker is not
     * accidentally contacted over TLS (calling {@code tlsPolicy()} always flips {@code useTls}
     * on). Per PIP-478 hostname verification defaults ON ({@link TlsPolicy}'s secure default);
     * an explicit {@code tlsEnableHostnameVerification=false} in the conf still opts out. When
     * {@code useKeyStoreTls=true}, the v4 keystore keys ({@code tlsTrustStorePath},
     * {@code tlsKeyStorePath}, ...) map onto a {@link TlsPolicy.Format#KEYSTORE} policy instead
     * of the PEM paths. The {@code jsseProvider} conf key is honored in both formats so a
     * FIPS/provider-specific endpoint (e.g. {@code BCJSSE}) is not silently served by the
     * platform provider.
     */
    @VisibleForTesting
    void applyTlsPolicy(PulsarClientBuilder clientBuilder, String serviceUrl, Properties properties) {
        boolean tlsByUrl = serviceUrl != null && serviceUrl.startsWith("pulsar+ssl://");
        boolean tlsByConf = Boolean.parseBoolean(properties.getProperty("useTls", "false"));
        if (!tlsByUrl && !tlsByConf) {
            return;
        }
        TlsPolicy.Builder tls = TlsPolicy.builder()
                .allowInsecureConnection(
                        Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false")));
        // Only an explicit conf value may override the builder's secure default (verification ON);
        // an absent key must not silently disable it.
        String hostnameVerification = properties.getProperty("tlsEnableHostnameVerification");
        if (hostnameVerification != null) {
            tls.enableHostnameVerification(Boolean.parseBoolean(hostnameVerification));
        }
        if (Boolean.parseBoolean(properties.getProperty("useKeyStoreTls", "false"))) {
            // The store types default to JKS, matching the v4 client.conf semantics; TlsPolicy's
            // null means KeyStore.getDefaultType() (PKCS12 on current JDKs), which would break
            // existing confs that rely on the v4 JKS default.
            tls.format(TlsPolicy.Format.KEYSTORE)
                    .trustStorePath(properties.getProperty("tlsTrustStorePath"))
                    .trustStorePassword(properties.getProperty("tlsTrustStorePassword"))
                    .trustStoreType(properties.getProperty("tlsTrustStoreType", "JKS"))
                    .keyStorePath(properties.getProperty("tlsKeyStorePath"))
                    .keyStorePassword(properties.getProperty("tlsKeyStorePassword"))
                    .keyStoreType(properties.getProperty("tlsKeyStoreType", "JKS"));
        } else {
            if (isNotBlank(rootParams.tlsTrustCertsFilePath)) {
                tls.trustCertsFilePath(rootParams.tlsTrustCertsFilePath);
            }
            String certFile = properties.getProperty("tlsCertificateFilePath");
            if (isNotBlank(certFile)) {
                tls.certificateFilePath(certFile);
            }
            String keyFile = properties.getProperty("tlsKeyFilePath");
            if (isNotBlank(keyFile)) {
                tls.keyFilePath(keyFile);
            }
        }
        // Format-independent: both provider axes apply to PEM and KEYSTORE alike.
        String jsseProvider = properties.getProperty("jsseProvider");
        if (isNotBlank(jsseProvider)) {
            tls.jsseProvider(jsseProvider);
        }
        String jcaProvider = properties.getProperty("jcaProvider");
        if (isNotBlank(jcaProvider)) {
            tls.jcaProvider(jcaProvider);
        }
        clientBuilder.tlsPolicy(tls.build());
    }

    public int run(String[] args) {
        return commander.execute(args);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: pulsar-client CONF_FILE_PATH [options] [command] [command options]");
            System.exit(1);
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        PulsarClientTool clientTool = new PulsarClientTool(properties);
        int exitCode = clientTool.run(Arrays.copyOfRange(args, 1, args.length));

        System.exit(exitCode);
    }

    @VisibleForTesting
    public void replaceProducerCommand(CmdProduce object) {
        this.produceCommand = object;
        if (commander.getSubcommands().containsKey("produce")) {
            commander.getCommandSpec().removeSubcommand("produce");
        }
        commander.addSubcommand("produce", this.produceCommand);
        enableCaseInsensitiveEnums();
    }

    @VisibleForTesting
    CommandLine getCommander() {
        return commander;
    }

    // The following methods are used for Pulsar shell.
    protected void setCommandName(String name) {
        commander.setCommandName(name);
    }

    protected String getServiceUrl() {
        return pulsarClientPropertiesProvider.getServiceUrl();
    }
}
