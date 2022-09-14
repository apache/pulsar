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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;
import org.apache.pulsar.admin.cli.utils.CustomCommandFactoryProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;

public class PulsarAdminTool {

    private static boolean allowSystemExit = true;

    private static int lastExitCode = Integer.MIN_VALUE;

    protected List<CustomCommandFactory> customCommandFactories = new ArrayList();
    protected Map<String, Class<?>> commandMap;
    protected JCommander jcommander;
    protected final PulsarAdminBuilder adminBuilder;
    protected RootParams rootParams;
    private final Properties properties;

    @Getter
    public static class RootParams {

        @Parameter(names = { "--admin-url" }, description = "Admin Service URL to which to connect.")
        String serviceUrl = null;

        @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
        String authPluginClassName = null;

        @Parameter(names = { "--request-timeout" }, description = "Request time out in seconds for "
                + "the pulsar admin client for any request")
        int requestTimeout = PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS;

        @Parameter(
            names = { "--auth-params" },
                description = "Authentication parameters, whose format is determined by the implementation "
                        + "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" "
                        + "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
        String authParams = null;

        @Parameter(names = { "--tls-allow-insecure" }, description = "Allow TLS insecure connection")
        Boolean tlsAllowInsecureConnection;

        @Parameter(names = { "--tls-trust-cert-path" }, description = "Allow TLS trust cert file path")
        String tlsTrustCertsFilePath;

        @Parameter(names = { "--tls-enable-hostname-verification" },
                description = "Enable TLS common name verification")
        Boolean tlsEnableHostnameVerification;

        @Parameter(names = {"--tls-provider"}, description = "Set up TLS provider. "
                + "When TLS authentication with CACert is used, the valid value is either OPENSSL or JDK. "
                + "When TLS authentication with KeyStore is used, available options can be SunJSSE, Conscrypt "
                + "and so on.")
        String tlsProvider;

        @Parameter(names = { "-v", "--version" }, description = "Get version of pulsar admin client")
        boolean version;

        @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
        boolean help;
    }

    public PulsarAdminTool(Properties properties) throws Exception {
        this.properties = properties;
        rootParams = new RootParams();
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        initRootParamsFromProperties(properties);
        adminBuilder = createAdminBuilder(properties);
        initJCommander();
    }

    protected PulsarAdminBuilder createAdminBuilder(Properties properties) {
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

        boolean tlsAllowInsecureConnection = this.rootParams.tlsAllowInsecureConnection != null
                ? this.rootParams.tlsAllowInsecureConnection
                : Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));

        boolean tlsEnableHostnameVerification = this.rootParams.tlsEnableHostnameVerification != null
                ? this.rootParams.tlsEnableHostnameVerification
                : Boolean.parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        final String tlsTrustCertsFilePath = isNotBlank(this.rootParams.tlsTrustCertsFilePath)
                ? this.rootParams.tlsTrustCertsFilePath
                : properties.getProperty("tlsTrustCertsFilePath");

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
                .tlsCertificateFilePath(tlsCertificateFilePath);
    }

    protected void initRootParamsFromProperties(Properties properties) {
        rootParams.serviceUrl = isNotBlank(properties.getProperty("webServiceUrl"))
                ? properties.getProperty("webServiceUrl")
                : properties.getProperty("serviceUrl");
        rootParams.authPluginClassName = properties.getProperty("authPlugin");
        rootParams.authParams = properties.getProperty("authParams");
    }

    private static class PulsarAdminSupplier implements Supplier<PulsarAdmin> {

        private final PulsarAdminBuilder pulsarAdminBuilder;
        private final Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory;
        private PulsarAdmin admin;
        private PulsarAdminSupplier(PulsarAdminBuilder pulsarAdminBuilder,
                                    Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
            this.pulsarAdminBuilder = pulsarAdminBuilder;
            this.adminFactory = adminFactory;
        }

        @Override
        public PulsarAdmin get() {
            if (admin == null) {
                admin = adminFactory.apply(pulsarAdminBuilder);
            }
            return admin;
        }
    }

    public void setupCommands(Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
        try {
            Supplier<PulsarAdmin> admin = new PulsarAdminSupplier(adminBuilder, adminFactory);
            for (Map.Entry<String, Class<?>> c : commandMap.entrySet()) {
                addCommand(c, admin);
            }

            CommandExecutionContext context = new CommandExecutionContext() {
                @Override
                public PulsarAdmin getPulsarAdmin() {
                    return admin.get();
                }

                @Override
                public Properties getConfiguration() {
                    return properties;
                }
            };
            loadCustomCommandFactories();

            for (CustomCommandFactory factory : customCommandFactories) {
                List<CustomCommandGroup> customCommandGroups = factory.commandGroups(context);
                for (CustomCommandGroup group : customCommandGroups) {
                    Object generated = CustomCommandsUtils.generateCliCommand(group, context, admin);
                    jcommander.addCommand(group.name(), generated);
                    commandMap.put(group.name(), null);
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

    private void loadCustomCommandFactories() throws Exception {
        customCommandFactories.addAll(CustomCommandFactoryProvider.createCustomCommandFactories(properties));
    }


    private void addCommand(Map.Entry<String, Class<?>> c, Supplier<PulsarAdmin> admin) throws Exception {
        // To remain backwards compatibility for "source" and "sink" commands
        // TODO eventually remove this
        if (c.getKey().equals("sources") || c.getKey().equals("source")) {
            jcommander.addCommand("sources", c.getValue().getConstructor(Supplier.class).newInstance(admin), "source");
        } else if (c.getKey().equals("sinks") || c.getKey().equals("sink")) {
            jcommander.addCommand("sinks", c.getValue().getConstructor(Supplier.class).newInstance(admin), "sink");
        } else if (c.getKey().equals("functions")) {
            jcommander.addCommand(c.getKey(), c.getValue().getConstructor(Supplier.class).newInstance(admin));
        } else {
            // Other mode, all components are initialized.
            if (c.getValue() != null) {
                jcommander.addCommand(c.getKey(), c.getValue().getConstructor(Supplier.class).newInstance(admin));
            }
        }
    }

    protected boolean run(String[] args) {
        final Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory = createAdminFactory(args);
        return run(args, adminFactory);
    }

    boolean run(String[] args, Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
        setupCommands(adminFactory);
        if (args.length == 0) {
            jcommander.usage();
            return false;
        }

        int cmdPos;
        for (cmdPos = 0; cmdPos < args.length; cmdPos++) {
            if (commandMap.containsKey(args[cmdPos])) {
                break;
            }
        }

        try {
            jcommander.parse(Arrays.copyOfRange(args, 0, Math.min(cmdPos, args.length)));

            //rootParams are populated by jcommander.parse
            adminBuilder.serviceHttpUrl(rootParams.serviceUrl);
            adminBuilder.authentication(rootParams.authPluginClassName, rootParams.authParams);
            adminBuilder.requestTimeout(rootParams.requestTimeout, TimeUnit.SECONDS);
            if (isBlank(rootParams.tlsProvider)) {
                rootParams.tlsProvider = properties.getProperty("webserviceTlsProvider");
            }
            if (isNotBlank(rootParams.tlsProvider)) {
                adminBuilder.sslProvider(rootParams.tlsProvider);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            jcommander.usage();
            return false;
        }

        if (isBlank(rootParams.serviceUrl)) {
            System.out.println("Can't find any admin url to use");
            jcommander.usage();
            return false;
        }

        if (rootParams.version) {
            System.out.println("Current version of pulsar admin client is: " + PulsarVersion.getVersion());
            return true;
        }

        if (rootParams.help) {
            jcommander.usage();
            return true;
        }

        if (cmdPos == args.length) {
            jcommander.usage();
            return false;
        } else {
            String cmd = args[cmdPos];

            // To remain backwards compatibility for "source" and "sink" commands
            // TODO eventually remove this
            if (cmd.equals("source")) {
                cmd = "sources";
            } else if (cmd.equals("sink")) {
                cmd = "sinks";
            }

            JCommander obj = jcommander.getCommands().get(cmd);
            CmdBase cmdObj = (CmdBase) obj.getObjects().get(0);

            return cmdObj.run(Arrays.copyOfRange(args, cmdPos + 1, args.length));
        }
    }

    public static void main(String[] args) throws Exception {
        lastExitCode = 0;
        if (args.length == 0) {
            System.out.println("Usage: pulsar-admin CONF_FILE_PATH [options] [command] [command options]");
            exit(0);
            return;
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
    }

    private static void exit(int code) {
        lastExitCode = code;
        if (allowSystemExit) {
            // we are using halt and not System.exit, we do not mind about shutdown hooks
            // they are only slowing down the tool
            Runtime.getRuntime().halt(code);
        } else {
            System.out.println("Exit code is " + code + " (System.exit not called, as we are in test mode)");
        }
    }

    private Function<PulsarAdminBuilder, ? extends PulsarAdmin> createAdminFactory(String[] args) {
        int cmdPos;
        for (cmdPos = 0; cmdPos < args.length; cmdPos++) {
            if (commandMap.containsKey(args[cmdPos])) {
                break;
            }
        }

        ++cmdPos;
        boolean isLocalRun = cmdPos < args.length && "localrun".equalsIgnoreCase(args[cmdPos]);

        Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory;
        if (isLocalRun) {
            // bypass constructing admin client
            adminFactory = (adminBuilder) -> null;
        } else {
            adminFactory = (adminBuilder) -> {
                try {
                    return adminBuilder.build();
                } catch (Exception ex) {
                    System.err.println(ex.getClass() + ": " + ex.getMessage());
                    exit(1);
                    return null;
                }
            };
        }
        return adminFactory;
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

    protected void initJCommander() {
        jcommander = new JCommander();
        jcommander.setProgramName("pulsar-admin");
        jcommander.addObject(rootParams);

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
        // TODO eventually remove this
        commandMap.put("source", CmdSources.class);
        commandMap.put("sink", CmdSinks.class);

        commandMap.put("packages", CmdPackages.class);
        commandMap.put("transactions", CmdTransactions.class);
    }

}
