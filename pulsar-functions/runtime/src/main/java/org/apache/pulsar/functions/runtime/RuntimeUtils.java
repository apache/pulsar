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

package org.apache.pulsar.functions.runtime;

import com.google.protobuf.util.JsonFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Util class for common runtime functionality
 */
@Slf4j
public class RuntimeUtils {

    private static final String FUNCTIONS_EXTRA_DEPS_PROPERTY = "pulsar.functions.extra.dependencies.dir";

    public static List<String> composeCmd(InstanceConfig instanceConfig,
                                          String instanceFile,
                                          String extraDependenciesDir, /* extra dependencies for running instances */
                                          String logDirectory,
                                          String originalCodeFileName,
                                          String pulsarServiceUrl,
                                          String stateStorageServiceUrl,
                                          AuthenticationConfig authConfig,
                                          String shardId,
                                          Integer grpcPort,
                                          Long expectedHealthCheckInterval,
                                          String logConfigFile,
                                          String secretsProviderClassName,
                                          String secretsProviderConfig,
                                          Boolean installUserCodeDependencies,
                                          String pythonDependencyRepository,
                                          String pythonExtraDependencyRepository,
                                          int metricsPort) throws Exception {

        final List<String> cmd = getArgsBeforeCmd(instanceConfig, extraDependenciesDir);

        cmd.addAll(getCmd(instanceConfig, instanceFile, extraDependenciesDir, logDirectory,
                originalCodeFileName, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig, shardId, grpcPort, expectedHealthCheckInterval,
                logConfigFile, secretsProviderClassName, secretsProviderConfig,
                installUserCodeDependencies, pythonDependencyRepository,
                pythonExtraDependencyRepository, metricsPort));
        return cmd;
    }

    public static List<String> getArgsBeforeCmd(InstanceConfig instanceConfig, String extraDependenciesDir) {

        final List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionDetails().getRuntime() ==  Function.FunctionDetails.Runtime.JAVA) {
            //no-op
        } else if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.PYTHON) {
            // add `extraDependenciesDir` to python package searching path
            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                args.add("PYTHONPATH=${PYTHONPATH}:" + extraDependenciesDir);
            }
        }

        return args;
    }

    public static List<String> getCmd(InstanceConfig instanceConfig,
                                          String instanceFile,
                                          String extraDependenciesDir, /* extra dependencies for running instances */
                                           String logDirectory,
                                          String originalCodeFileName,
                                          String pulsarServiceUrl,
                                          String stateStorageServiceUrl,
                                          AuthenticationConfig authConfig,
                                          String shardId,
                                          Integer grpcPort,
                                          Long expectedHealthCheckInterval,
                                          String logConfigFile,
                                          String secretsProviderClassName,
                                          String secretsProviderConfig,
                                          Boolean installUserCodeDependencies,
                                          String pythonDependencyRepository,
                                          String pythonExtraDependencyRepository,
                                          int metricsPort) throws Exception {
        final List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionDetails().getRuntime() ==  Function.FunctionDetails.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");

            String classpath = instanceFile;
            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                classpath = classpath + ":" + extraDependenciesDir + "/*";
            }
            args.add(classpath);

            // Keep the same env property pointing to the Java instance file so that it can be picked up
            // by the child process and manually added to classpath
            args.add(String.format("-D%s=%s", FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY, instanceFile));
            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                args.add(String.format("-D%s=%s", FUNCTIONS_EXTRA_DEPS_PROPERTY, extraDependenciesDir));
            }
            args.add("-Dlog4j.configurationFile=" + logConfigFile);
            args.add("-Dpulsar.function.log.dir=" + genFunctionLogFolder(logDirectory, instanceConfig));
            args.add("-Dpulsar.function.log.file=" + String.format(
                    "%s-%s",
                    instanceConfig.getFunctionDetails().getName(),
                    shardId));
            if (instanceConfig.getFunctionDetails().getResources() != null) {
                Function.Resources resources = instanceConfig.getFunctionDetails().getResources();
                if (resources.getRam() != 0) {
                    args.add("-Xmx" + String.valueOf(resources.getRam()));
                }
            }
            args.add(JavaInstanceMain.class.getName());
            args.add("--jar");
            args.add(originalCodeFileName);
        } else if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.PYTHON) {
            args.add("python");
            args.add(instanceFile);
            args.add("--py");
            args.add(originalCodeFileName);
            args.add("--logging_directory");
            args.add(logDirectory);
            args.add("--logging_file");
            args.add(instanceConfig.getFunctionDetails().getName());
            // set logging config file
            args.add("--logging_config_file");
            args.add(logConfigFile);
            // `installUserCodeDependencies` is only valid for python runtime
            if (installUserCodeDependencies != null && installUserCodeDependencies) {
                args.add("--install_usercode_dependencies");
                args.add("True");
            }
            if (!isEmpty(pythonDependencyRepository)) {
                args.add("--dependency_repository");
                args.add(pythonDependencyRepository);
            }
            if (!isEmpty(pythonExtraDependencyRepository)) {
                args.add("--extra_dependency_repository");
                args.add(pythonExtraDependencyRepository);
            }
            // TODO:- Find a platform independent way of controlling memory for a python application
        }
        args.add("--instance_id");
        args.add(shardId);
        args.add("--function_id");
        args.add(instanceConfig.getFunctionId());
        args.add("--function_version");
        args.add(instanceConfig.getFunctionVersion());
        args.add("--function_details");
        args.add("'" + JsonFormat.printer().omittingInsignificantWhitespace().print(instanceConfig.getFunctionDetails()) + "'");

        args.add("--pulsar_serviceurl");
        args.add(pulsarServiceUrl);
        if (authConfig != null) {
            if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                    && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                args.add("--client_auth_plugin");
                args.add(authConfig.getClientAuthenticationPlugin());
                args.add("--client_auth_params");
                args.add(authConfig.getClientAuthenticationParameters());
            }
            args.add("--use_tls");
            args.add(Boolean.toString(authConfig.isUseTls()));
            args.add("--tls_allow_insecure");
            args.add(Boolean.toString(authConfig.isTlsAllowInsecureConnection()));
            args.add("--hostname_verification_enabled");
            args.add(Boolean.toString(authConfig.isTlsHostnameVerificationEnable()));
            if (isNotBlank(authConfig.getTlsTrustCertsFilePath())) {
                args.add("--tls_trust_cert_path");
                args.add(authConfig.getTlsTrustCertsFilePath());
            }
        }
        args.add("--max_buffered_tuples");
        args.add(String.valueOf(instanceConfig.getMaxBufferedTuples()));

        args.add("--port");
        args.add(String.valueOf(grpcPort));

        args.add("--metrics_port");
        args.add(String.valueOf(metricsPort));

        // state storage configs
        if (null != stateStorageServiceUrl) {
            args.add("--state_storage_serviceurl");
            args.add(stateStorageServiceUrl);
        }
        args.add("--expected_healthcheck_interval");
        args.add(String.valueOf(expectedHealthCheckInterval));

        if (!StringUtils.isEmpty(secretsProviderClassName)) {
            args.add("--secrets_provider");
            args.add(secretsProviderClassName);
            if (!StringUtils.isEmpty(secretsProviderConfig)) {
                args.add("--secrets_provider_config");
                args.add("'" + secretsProviderConfig + "'");
            }
        }

        args.add("--cluster_name");
        args.add(instanceConfig.getClusterName());
        return args;
    }

    public static String genFunctionLogFolder(String logDirectory, InstanceConfig instanceConfig) {
        return String.format(
                "%s/%s",
                logDirectory,
                FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
    }

    public static String getPrometheusMetrics(int metricsPort) throws IOException{
        StringBuilder result = new StringBuilder();
        URL url = new URL(String.format("http://%s:%s", InetAddress.getLocalHost().getHostAddress(), metricsPort));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line + System.lineSeparator());
        }
        rd.close();
        return result.toString();
    }

    public static class Actions {
        private List<Action> actions = new LinkedList<>();

        @Data
        @Builder(toBuilder=true)
        public static class Action {
            private String actionName;
            private int numRetries = 1;
            private Supplier<ActionResult> supplier;
            private long sleepBetweenInvocationsMs = 500;
            private Boolean continueOn;
            private Runnable onFail;
            private Runnable onSuccess;

            public void verifyAction() {
                if (isBlank(actionName)) {
                    throw new RuntimeException("Action name is empty!");
                }
                if (supplier == null) {
                    throw new RuntimeException("Supplier is not specified!");
                }
            }
        }

        @Data
        @Builder
        public static class ActionResult {
            private boolean success;
            private String errorMsg;
        }

        private Actions() {

        }


        public Actions addAction(Action action) {
            action.verifyAction();
            this.actions.add(action);
            return this;
        }

        public static Actions newBuilder() {
            return new Actions();
        }

        public int numActions() {
            return actions.size();
        }

        public void run() throws InterruptedException {
            Iterator<Action> it = this.actions.iterator();
            while(it.hasNext()) {
                Action action  = it.next();

                boolean success;
                try {
                    success = runAction(action);
                } catch (Exception e) {
                    log.error("Uncaught exception thrown when running action [ {} ]:", action.getActionName(), e);
                    success = false;
                }
                if (action.getContinueOn() != null
                        && success == action.getContinueOn()) {
                    continue;
                } else {
                    // terminate
                    break;
                }
            }
        }

        private boolean runAction(Action action) throws InterruptedException {
            for (int i = 0; i< action.getNumRetries(); i++) {

                ActionResult actionResult = action.getSupplier().get();

                if (actionResult.isSuccess()) {
                    log.info("Sucessfully completed action [ {} ]", action.getActionName());
                    if (action.getOnSuccess() != null) {
                        action.getOnSuccess().run();
                    }
                    return true;
                } else {
                    if (actionResult.getErrorMsg() != null) {
                        log.warn("Error completing action [ {} ] :- {} - [ATTEMPT] {}/{}",
                                action.getActionName(),
                                actionResult.getErrorMsg(),
                                i + 1, action.getNumRetries());
                    } else {
                        log.warn("Error completing action [ {} ] [ATTEMPT] {}/{}",
                                action.getActionName(),
                                i + 1, action.getNumRetries());
                    }

                    Thread.sleep(action.sleepBetweenInvocationsMs);
                }
            }
            log.error("Failed completing action [ {} ]. Giving up!", action.getActionName());
            if (action.getOnFail() != null) {
                action.getOnFail().run();
            }
            return false;
        }
    }
}
