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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;

public class PulsarAdminTool {

    private static boolean allowSystemExit = true;

    private static int lastExitCode = Integer.MIN_VALUE;

    protected final Map<String, Class<?>> commandMap;
    private final JCommander jcommander;
    protected final PulsarAdminBuilder adminBuilder;

    @Parameter(names = { "--admin-url" }, description = "Admin Service URL to which to connect.")
    String serviceUrl = null;

    @Parameter(names = { "--auth-plugin" }, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = { "--request-timeout" }, description = "Request time out in seconds for "
            + "the pulsar admin client for any request")
    int requestTimeout = PulsarAdminImpl.DEFAULT_REQUEST_TIMEOUT_SECONDS;

    @Parameter(
        names = { "--auth-params" },
        description = "Authentication parameters, whose format is determined by the implementation " +
            "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
            "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
    String authParams = null;

    @Parameter(names = { "--tls-allow-insecure" }, description = "Allow TLS insecure connection")
    Boolean tlsAllowInsecureConnection;

    @Parameter(names = { "--tls-trust-cert-path" }, description = "Allow TLS trust cert file path")
    String tlsTrustCertsFilePath;

    @Parameter(names = { "--tls-enable-hostname-verification" }, description = "Enable TLS common name verification")
    Boolean tlsEnableHostnameVerification;

    @Parameter(names = { "-v", "--version" }, description = "Get version of pulsar admin client")
    boolean version;

    @Parameter(names = { "-h", "--help", }, help = true, description = "Show this help.")
    boolean help;

    // for tls with keystore type config
    boolean useKeyStoreTls;
    String tlsTrustStoreType;
    String tlsTrustStorePath;
    String tlsTrustStorePassword;

    PulsarAdminTool(Properties properties) throws Exception {
        // fallback to previous-version serviceUrl property to maintain backward-compatibility
        serviceUrl = StringUtils.isNotBlank(properties.getProperty("webServiceUrl"))
                ? properties.getProperty("webServiceUrl")
                : properties.getProperty("serviceUrl");
        authPluginClassName = properties.getProperty("authPlugin");
        authParams = properties.getProperty("authParams");
        boolean tlsAllowInsecureConnection = this.tlsAllowInsecureConnection != null ? this.tlsAllowInsecureConnection
                : Boolean.parseBoolean(properties.getProperty("tlsAllowInsecureConnection", "false"));

        boolean tlsEnableHostnameVerification = this.tlsEnableHostnameVerification != null
                ? this.tlsEnableHostnameVerification
                : Boolean.parseBoolean(properties.getProperty("tlsEnableHostnameVerification", "false"));
        final String tlsTrustCertsFilePath = StringUtils.isNotBlank(this.tlsTrustCertsFilePath)
                ? this.tlsTrustCertsFilePath
                : properties.getProperty("tlsTrustCertsFilePath");

        this.useKeyStoreTls = Boolean
                .parseBoolean(properties.getProperty("useKeyStoreTls", "false"));
        this.tlsTrustStoreType = properties.getProperty("tlsTrustStoreType", "JKS");
        this.tlsTrustStorePath = properties.getProperty("tlsTrustStorePath");
        this.tlsTrustStorePassword = properties.getProperty("tlsTrustStorePassword");

        adminBuilder = PulsarAdmin.builder().allowTlsInsecureConnection(tlsAllowInsecureConnection)
                .enableTlsHostnameVerification(tlsEnableHostnameVerification)
                .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                .useKeyStoreTls(useKeyStoreTls)
                .tlsTrustStoreType(tlsTrustStoreType)
                .tlsTrustStorePath(tlsTrustStorePath)
                .tlsTrustStorePassword(tlsTrustStorePassword);

        jcommander = new JCommander();
        jcommander.setProgramName("pulsar-admin");
        jcommander.addObject(this);

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
    }

    private static class PulsarAdminSupplier implements Supplier<PulsarAdmin> {

        private final PulsarAdminBuilder pulsarAdminBuilder;
        private final Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory;
        private PulsarAdmin admin;
        private PulsarAdminSupplier(PulsarAdminBuilder pulsarAdminBuilder, Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
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

    private void setupCommands(Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
        try {
            adminBuilder.serviceHttpUrl(serviceUrl);
            adminBuilder.authentication(authPluginClassName, authParams);
            adminBuilder.requestTimeout(requestTimeout, TimeUnit.SECONDS);
            Supplier<PulsarAdmin> admin = new PulsarAdminSupplier(adminBuilder, adminFactory);
            for (Map.Entry<String, Class<?>> c : commandMap.entrySet()) {
                addCommand(c, admin);
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
            jcommander.addCommand(c.getKey(), c.getValue().getConstructor(Supplier.class).newInstance(admin));
        }
    }

    boolean run(String[] args) {
        return run(args, adminBuilder -> {
            try {
                return adminBuilder.build();
            } catch (Exception ex) {
                System.err.println(ex.getClass() + ": " + ex.getMessage());
                System.exit(1);
                return null;
            }
        });
    }

    boolean run(String[] args, Function<PulsarAdminBuilder, ? extends PulsarAdmin> adminFactory) {
        if (args.length == 0) {
            setupCommands(adminFactory);
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
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            setupCommands(adminFactory);
            jcommander.usage();
            return false;
        }

        if (version) {
            System.out.println("Current version of pulsar admin client is: " + PulsarVersion.getVersion());
            return true;
        }

        if (help) {
            setupCommands(adminFactory);
            jcommander.usage();
            return true;
        }

        if (cmdPos == args.length) {
            setupCommands(adminFactory);
            jcommander.usage();
            return false;
        } else {
            setupCommands(adminFactory);
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
        String configFile = null;
        if (args.length > 0) {
            configFile = args[0];
            args = Arrays.copyOfRange(args, 1, args.length);
        }
        Properties properties = new Properties();

        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
        }

        PulsarAdminTool tool = new PulsarAdminTool(properties);

        int cmdPos;
        for (cmdPos = 0; cmdPos < args.length; cmdPos++) {
            if (tool.commandMap.containsKey(args[cmdPos])) {
                break;
            }
        }

        ++cmdPos;
        boolean isLocalRun = cmdPos < args.length && "localrun".equals(args[cmdPos].toLowerCase());

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

        if (tool.run(args, adminFactory)) {
            exit(0);
        } else {
            exit(1);
        }
    }

    private static void exit(int code) {
        lastExitCode = lastExitCode;
        if (allowSystemExit) {
            // we are using halt and not System.exit, we do not mind about shutdown hooks
            // they are only slowing down the tool
            Runtime.getRuntime().halt(code);
        } else {
            System.out.println("Exit code is " + code+" (System.exit not called, as we are in test mode)");
        }
    }

    static void setAllowSystemExit(boolean allowSystemExit) {
        PulsarAdminTool.allowSystemExit = allowSystemExit;
    }

    static int getLastExitCode() {
        return lastExitCode;
    }


}
