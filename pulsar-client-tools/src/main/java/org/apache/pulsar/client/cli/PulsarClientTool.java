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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import lombok.Getter;
import org.apache.pulsar.cli.picocli.ByteUnitToLongConverter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.PropertiesDefaultProvider;
import picocli.CommandLine.ScopeType;

@Command(
        name = "pulsar-client",
        mixinStandardHelpOptions = true,
        versionProvider = PulsarVersionProvider.class,
        scope = ScopeType.INHERIT
)
public class PulsarClientTool {

    @Getter
    @Command(description = "Produce or consume messages on a specified topic")
    public static class RootParams {
        @Option(names = {"--url"}, descriptionKey = "brokerServiceUrl",
                description = "Broker URL to which to connect.")
        String serviceURL = null;

        @Option(names = {"--proxy-url"}, descriptionKey = "proxyServiceUrl",
                description = "Proxy-server URL to which to connect.")
        String proxyServiceURL = null;

        @Option(names = {"--proxy-protocol"}, description = "Proxy protocol to select type of routing at proxy.",
                converter = ProxyProtocolConverter.class)
        ProxyProtocol proxyProtocol = null;

        @Option(names = {"--auth-plugin"}, descriptionKey = "authPlugin", description = "Authentication plugin class name.")
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

        @Option(names = {"-v", "--version"}, description = "Get version of pulsar client")
        boolean version;

        @Option(names = {"-h", "--help",}, help = true, description = "Show this help.")
        boolean help;

        @Option(names = {"--tlsTrustCertsFilePath"},
                descriptionKey = "tlsTrustCertsFilePath",
                description = "File path to client trust certificates")
        String tlsTrustCertsFilePath;

        @Option(names = {"-ml", "--memory-limit",}, description = "Configure the Pulsar client memory limit "
                + "(eg: 32M, 64M)", descriptionKey = "memoryLimit", converter = ByteUnitToLongConverter.class)
        long memoryLimit = 0L;
    }

    private static final String brokerServiceUrlKey = "brokerServiceUrl";

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

    private CommandLine commander;
    protected CmdProduce produceCommand;
    protected CmdConsume consumeCommand;
    protected CmdRead readCommand;
    CmdGenerateDocumentation generateDocumentation;
    private Properties properties;

    public PulsarClientTool(Properties properties) {
        setProperties(properties);
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

        initCommander();
    }

    private void printHelp() {
        commander.printVersionHelp(System.out);
    }

    protected void initCommander() {
        // Use -v instead -V
        System.setProperty("picocli.version.name.0", "-v");

        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();
        readCommand = new CmdRead();
        generateDocumentation = new CmdGenerateDocumentation();

        commander = new CommandLine(this);
        commander.setDefaultValueProvider(new PropertiesDefaultProvider(properties));
        commander.addSubcommand("produce", produceCommand);
        commander.addSubcommand("consume", consumeCommand);
        commander.addSubcommand("read", readCommand);
        commander.addSubcommand("generate_documentation", generateDocumentation);
    }

    protected void addCommand(String name, Object cmd) {
        commander.addSubcommand(name, cmd);
    }

    private void updateConfig() throws UnsupportedAuthenticationException {
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

        if (isNotBlank(rootParams.proxyServiceURL)) {
            if (rootParams.proxyProtocol == null) {
                commander.getErr().println("proxy-protocol must be provided with proxy-url");
                System.exit(1);
            }
            clientBuilder.proxyServiceUrl(rootParams.proxyServiceURL, rootParams.proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
        this.readCommand.updateConfig(clientBuilder, authentication, this.rootParams.serviceURL);
    }

    public int run(String[] args) {
        commander.setExecutionStrategy(parseResult -> {
            try {
                updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                // it will default to the values passed in by the constructor
            } catch (UnsupportedAuthenticationException exp) {
                commander.getErr().println("Failed to load an authentication plugin");
                exp.printStackTrace();
                return -1;
            }

            return new CommandLine.RunLast().execute(parseResult);
        });
        return commander.execute(args);
    }

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String[] finallyArgs = args;
        if (args.length != 0) {
            Path configFilePath = null;
            String configFile = args[0];
            try {
                configFilePath = Paths.get(configFile);
            } catch (InvalidPathException ignore) {
                // Not config file path
            }
            if (configFilePath != null) {
                if (Files.isReadable(configFilePath)) {
                    try (FileInputStream fis = new FileInputStream(configFilePath.toString())) {
                        properties.load(fis);
                    }
                    finallyArgs = Arrays.copyOfRange(args, 1, args.length);
                }
            }
        } else {

        }

        PulsarClientTool clientTool = new PulsarClientTool(properties);
        if (args.length == 0) {
            clientTool.printHelp();
            System.exit(0);
            return;
        }
        int exitCode = clientTool.run(finallyArgs);
        System.exit(exitCode);
    }

    // The following methods are used for Pulsar shell.
    protected void setCommandName(String name) {
        commander.setCommandName(name);
    }

    protected String getServiceUrl() {
        return properties.getProperty(brokerServiceUrlKey);
    }

    protected void setProperties(Properties properties) {
        String brokerServiceUrl = properties.getProperty(brokerServiceUrlKey);
        if (isBlank(brokerServiceUrl)) {
            String serviceUrl = properties.getProperty("webServiceUrl");
            if (isBlank(serviceUrl)) {
                // fallback to previous-version serviceUrl property to maintain backward-compatibility
                serviceUrl = properties.getProperty("serviceUrl");
            }
            if (isNotBlank(serviceUrl)) {
                properties.put(brokerServiceUrlKey, serviceUrl);
            }
        }

        this.properties = properties;
        if (commander != null) {
            commander.setDefaultValueProvider(new PropertiesDefaultProvider(properties));
        }
    }

    @VisibleForTesting
    public void replaceProducerCommand(Object object) {
        if (!(object instanceof CmdProduce)) {
            throw new IllegalArgumentException("This object must be an instance of CmdProduce.");
        }
        this.produceCommand = (CmdProduce) object;
        if (commander.getSubcommands().containsKey("produce")) {
            commander.getCommandSpec().removeSubcommand("produce");
        }
        commander.addSubcommand("produce", this.produceCommand);
    }

    @VisibleForTesting
    public CommandLine getPicocliCommander() {
        return commander;
    }
}
