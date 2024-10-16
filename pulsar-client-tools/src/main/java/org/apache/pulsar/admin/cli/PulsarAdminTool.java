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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;
import org.apache.pulsar.admin.cli.utils.CustomCommandFactoryProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.internal.CommandHook;
import org.apache.pulsar.internal.CommanderFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Command(name = "pulsar-admin",
        scope = ScopeType.INHERIT,
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        versionProvider = PulsarVersionProvider.class
)
public class PulsarAdminTool implements CommandHook {

    protected static boolean allowSystemExit = true;

    private static int lastExitCode = Integer.MIN_VALUE;

    protected List<CustomCommandFactory> customCommandFactories;
    protected Map<String, Class<?>> commandMap;
    protected final CommandLine commander;
    @ArgGroup(heading = "Options:%n", exclusive = false)
    protected RootParams rootParams = new RootParams();
    protected PulsarAdminSupplier pulsarAdminSupplier;
    private PulsarAdminPropertiesProvider pulsarAdminPropertiesProvider;

    @Getter
    public static class RootParams {

        @Option(names = { "--admin-url" }, description = "Admin Service URL to which to connect.",
                descriptionKey = "webServiceUrl")
        String serviceUrl = null;

        @Option(names = { "--auth-plugin" }, description = "Authentication plugin class name.",
                descriptionKey = "authPlugin")
        String authPluginClassName = null;

        @Option(names = { "--request-timeout" }, description = "Request time out in seconds for "
                + "the pulsar admin client for any request")
        int requestTimeout = PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS;

        @Option(names = { "--auth-params" }, descriptionKey = "authParams",
                description = "Authentication parameters, whose format is determined by the implementation "
                        + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                        + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}\".")
        String authParams = null;

        @Option(names = { "--tls-allow-insecure" }, description = "Allow TLS insecure connection")
        Boolean tlsAllowInsecureConnection;

        @Option(names = { "--tls-trust-cert-path" }, description = "Allow TLS trust cert file path")
        String tlsTrustCertsFilePath;

        @Option(names = { "--tls-enable-hostname-verification" },
                description = "Enable TLS common name verification")
        Boolean tlsEnableHostnameVerification;

        @Option(names = {"--tls-provider"}, description = "Set up TLS provider. "
                + "When TLS authentication with CACert is used, the valid value is either OPENSSL or JDK. "
                + "When TLS authentication with KeyStore is used, available options can be SunJSSE, Conscrypt "
                + "and so on.", descriptionKey = "webserviceTlsProvider")
        String tlsProvider;
    }

    public PulsarAdminTool(Properties properties) throws Exception {
        // Use -v instead -V
        System.setProperty("picocli.version.name.0", "-v");
        commander = CommanderFactory.createRootCommanderWithHook(this, pulsarAdminPropertiesProvider);
        pulsarAdminSupplier = new PulsarAdminSupplier(createAdminBuilderFromProperties(properties), rootParams);
        initCommander(properties);
    }

    private static PulsarAdminBuilder createAdminBuilderFromProperties(Properties properties) {
        boolean useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        String tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        String tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        String tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");
        String tlsKeyStoreType = properties.getProperty("tlsKeyStoreType", "JKS");
        String tlsKeyStorePath = properties.getProperty("tlsKeyStorePath");
        String tlsKeyStorePassword = properties.getProperty("tlsKeyStorePassword");
        String tlsKeyFilePath = properties.getProperty("tlsKeyFilePath");
        String tlsCertificateFilePath = properties.getProperty("tlsCertificateFilePath");

        boolean tlsAllowInsecureConnection = Boolean.parseBoolean(properties
                .getProperty("tlsAllowInsecureConnection", "false"));

        boolean tlsEnableHostnameVerification = Boolean.parseBoolean(properties
                .getProperty("tlsEnableHostnameVerification", "false"));
        final String tlsTrustCertsFilePath = properties.getProperty("tlsTrustCertsFilePath");
        final String sslFactoryPlugin = properties.getProperty("sslFactoryPlugin",
                DefaultPulsarSslFactory.class.getName());
        final String sslFactoryPluginParams = properties.getProperty("sslFactoryPluginParams", "");

        return PulsarAdmin.builder().allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .enableTlsHostnameVerification(tlsEnableHostnameVerification)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword)
                .tlsKeyStoreType(tlsKeyStoreType)
                .tlsKeyStorePath(tlsKeyStorePath)
                .tlsKeyStorePassword(tlsKeyStorePassword)
                .tlsKeyFilePath(tlsKeyFilePath)
                .tlsCertificateFilePath(tlsCertificateFilePath)
                .sslFactoryPlugin(sslFactoryPlugin)
                .sslFactoryPluginParams(sslFactoryPluginParams);
    }

    private void setupCommands(Properties properties) {
        try {
            for (Map.Entry<String, Class<?>> c : commandMap.entrySet()) {
                Object o = c.getValue().getConstructor(Supplier.class).newInstance(pulsarAdminSupplier);
                addCommand(c.getKey(), o);
            }

            CommandExecutionContext context = new CommandExecutionContext() {
                @Override
                public PulsarAdmin getPulsarAdmin() {
                    return pulsarAdminSupplier.get();
                }

                @Override
                public Properties getConfiguration() {
                    return properties;
                }
            };

            for (CustomCommandFactory factory : customCommandFactories) {
                List<CustomCommandGroup> customCommandGroups = factory.commandGroups(context);
                for (CustomCommandGroup group : customCommandGroups) {
                    Object generated = CustomCommandsUtils.generateCliCommand(group, context, pulsarAdminSupplier);
                    addCommand(group.name(), generated);
                }
            }
        } catch (Exception e) {
            Throwable cause;
            if (e instanceof InvocationTargetException && null != e.getCause()) {
                cause = e.getCause();
            } else {
                cause = e;
            }
            System.err.println(cause.getClass() + ": " + cause.getMessage());
            System.exit(1);
        }
    }

    private void addCommand(String name, Object o) throws Exception {
        if (o instanceof CmdBase) {
            commander.addSubcommand(name, ((CmdBase) o).getCommander());
        } else {
            commander.addSubcommand(o);
        }
    }

    protected boolean run(String[] args) {
        return commander.execute(args) == 0;
    }

    public static void main(String[] args) throws Exception {
        execute(args);
    }

    @VisibleForTesting
    public static PulsarAdminTool execute(String[] args) throws Exception {
        lastExitCode = 0;
        if (args.length == 0) {
            System.out.println("Usage: pulsar-admin CONF_FILE_PATH [options] [command] [command options]");
            exit(0);
            return null;
        }
        String configFile = args[0];
        Properties properties = new Properties();

        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        PulsarAdminTool tool = new PulsarAdminTool(properties);
        args = Arrays.copyOfRange(args, 1, args.length);
        if (tool.run(args)) {
            exit(0);
        } else {
            exit(1);
        }
        return tool;
    }

    private static void exit(int code) {
        lastExitCode = code;
        if (allowSystemExit) {
            // we are using halt and not System.exit, we do not mind about shutdown hooks
            // they are only slowing down the tool
            ShutdownUtil.triggerImmediateForcefulShutdown(code, false);
        } else {
            System.out.println("Exit code is " + code + " (System.exit not called, as we are in test mode)");
        }
    }

    static void setAllowSystemExit(boolean allowSystemExit) {
        PulsarAdminTool.allowSystemExit = allowSystemExit;
    }

    static int getLastExitCode() {
        return lastExitCode;
    }

    @VisibleForTesting
    static void resetLastExitCode() {
        lastExitCode = Integer.MIN_VALUE;
    }

    @Override
    public int preRun() {
        if (isBlank(rootParams.serviceUrl)) {
            commander.getErr().println("Can't find any admin url to use");
            return 1;
        }
        pulsarAdminSupplier.rootParamsUpdated(rootParams);
        return 0;
    }

    private void initCommander(Properties properties) throws IOException {
        customCommandFactories = CustomCommandFactoryProvider.createCustomCommandFactories(properties);
        pulsarAdminPropertiesProvider = PulsarAdminPropertiesProvider.create(properties);
        commander.setDefaultValueProvider(pulsarAdminPropertiesProvider);
        commandMap = new HashMap<>();
        commandMap.put("clusters", CmdClusters.class);
        commandMap.put("ns-isolation-policy", CmdNamespaceIsolationPolicy.class);
        commandMap.put("brokers", CmdBrokers.class);
        commandMap.put("broker-stats", CmdBrokerStats.class);
        commandMap.put("tenants", CmdTenants.class);
        commandMap.put("resourcegroups", CmdResourceGroups.class);
        commandMap.put("properties", CmdTenants.CmdProperties.class); // deprecated, doesn't show in usage()
        commandMap.put("namespaces", CmdNamespaces.class);
        commandMap.put("topics", CmdTopics.class);
        commandMap.put("topicPolicies", CmdTopicPolicies.class);
        commandMap.put("schemas", CmdSchemas.class);
        commandMap.put("bookies", CmdBookies.class);

        // Hidden deprecated "persistent" and "non-persistent" subcommands
        commandMap.put("persistent", CmdPersistentTopics.class);
        commandMap.put("non-persistent", CmdNonPersistentTopics.class);


        commandMap.put("resource-quotas", CmdResourceQuotas.class);
        // pulsar-proxy cli
        commandMap.put("proxy-stats", CmdProxyStats.class);

        commandMap.put("functions", CmdFunctions.class);
        commandMap.put("functions-worker", CmdFunctionWorker.class);
        commandMap.put("sources", CmdSources.class);
        commandMap.put("sinks", CmdSinks.class);

        // Automatically generate documents for pulsar-admin
        commandMap.put("documents", CmdGenerateDocument.class);
        // To remain backwards compatibility for "source" and "sink" commands
        commandMap.put("packages", CmdPackages.class);
        commandMap.put("transactions", CmdTransactions.class);

        setupCommands(properties);
    }

    @VisibleForTesting
    public PulsarAdminSupplier getPulsarAdminSupplier() {
        return pulsarAdminSupplier;
    }

    // The following methods are used for Pulsar shell.
    protected void setCommandName(String name) {
        commander.setCommandName(name);
    }

    protected String getAdminUrl() {
        return pulsarAdminPropertiesProvider.getAdminUrl();
    }
}
