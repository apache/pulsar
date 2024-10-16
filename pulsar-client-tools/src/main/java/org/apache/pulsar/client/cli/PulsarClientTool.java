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
import java.util.Properties;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.internal.CommandHook;
import org.apache.pulsar.internal.CommanderFactory;
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
    boolean tlsAllowInsecureConnection;
    boolean tlsEnableHostnameVerification;

    String tlsKeyFilePath;
    String tlsCertificateFilePath;


    // for tls with keystore type config
    boolean useKeyStoreTls;
    String tlsTrustStoreType;
    String tlsTrustStorePath;
    String tlsTrustStorePassword;
    String tlsKeyStoreType;
    String tlsKeyStorePath;
    String tlsKeyStorePassword;
    String sslFactoryPlugin;
    String sslFactoryPluginParams;

    protected final CommandLine commander;
    protected CmdProduce produceCommand;
    protected CmdConsume consumeCommand;
    protected CmdRead readCommand;
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
        generateDocumentation = new CmdGenerateDocumentation();

        this.tlsAllowInsecureConnection = Boolean
                .parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));
        this.tlsEnableHostnameVerification = Boolean
                .parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        this.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        this.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        this.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        this.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");

        this.tlsKeyStoreType = properties.getProperty("tlsKeyStoreType", "JKS");
        this.tlsKeyStorePath = properties.getProperty("tlsKeyStorePath");
        this.tlsKeyStorePassword = properties.getProperty("tlsKeyStorePassword");
        this.tlsKeyFilePath = properties.getProperty("tlsKeyFilePath");
        this.tlsCertificateFilePath = properties.getProperty("tlsCertificateFilePath");
        this.sslFactoryPlugin = properties.getProperty("sslFactoryPlugin");
        this.sslFactoryPluginParams = properties.getProperty("sslFactoryPluginParams");

        pulsarClientPropertiesProvider = PulsarClientPropertiesProvider.create(properties);
        commander.setDefaultValueProvider(pulsarClientPropertiesProvider);
        commander.addSubcommand("produce", produceCommand);
        commander.addSubcommand("consume", consumeCommand);
        commander.addSubcommand("read", readCommand);
        commander.addSubcommand("generate_documentation", generateDocumentation);
    }

    protected void addCommand(String name, Object cmd) {
        commander.addSubcommand(name, cmd);
    }

    private int updateConfig() throws UnsupportedAuthenticationException {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(rootParams.memoryLimit, SizeUnit.BYTES);
        Authentication authentication = null;
        if (isNotBlank(this.rootParams.authPluginClassName)) {
            authentication = AuthenticationFactory.create(rootParams.authPluginClassName, rootParams.authParams);
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.rootParams.listenerName)) {
            clientBuilder.listenerName(this.rootParams.listenerName);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.enableTlsHostnameVerification(this.tlsEnableHostnameVerification);
        clientBuilder.serviceUrl(rootParams.serviceURL);

        clientBuilder.tlsTrustCertsFilePath(this.rootParams.tlsTrustCertsFilePath)
                .tlsKeyFilePath(tlsKeyFilePath)
                .tlsCertificateFilePath(tlsCertificateFilePath);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword)
                .tlsKeyStoreType(tlsKeyStoreType)
                .tlsKeyStorePath(tlsKeyStorePath)
                .tlsKeyStorePassword(tlsKeyStorePassword);

        clientBuilder.sslFactoryPlugin(sslFactoryPlugin)
                .sslFactoryPluginParams(sslFactoryPluginParams);

        if (isNotBlank(rootParams.proxyServiceURL)) {
            if (rootParams.proxyProtocol == null) {
                commander.getErr().println("proxy-protocol must be provided with proxy-url");
                return 1;
            }
            clientBuilder.proxyServiceUrl(rootParams.proxyServiceURL, rootParams.proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.readCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        return 0;
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
