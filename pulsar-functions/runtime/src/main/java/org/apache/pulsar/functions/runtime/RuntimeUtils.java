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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.instance.go.GoInstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionCommon;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Util class for common runtime functionality.
 */
@Slf4j
public class RuntimeUtils {

    private static final String FUNCTIONS_EXTRA_DEPS_PROPERTY = "pulsar.functions.extra.dependencies.dir";
    public static final String FUNCTIONS_INSTANCE_CLASSPATH = "pulsar.functions.instance.classpath";

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
                                          int metricsPort,
                                          String narExtractionDirectory) throws Exception {

        final List<String> cmd = getArgsBeforeCmd(instanceConfig, extraDependenciesDir);

        cmd.addAll(getCmd(instanceConfig, instanceFile, extraDependenciesDir, logDirectory,
                originalCodeFileName, pulsarServiceUrl, stateStorageServiceUrl,
                authConfig, shardId, grpcPort, expectedHealthCheckInterval,
                logConfigFile, secretsProviderClassName, secretsProviderConfig,
                installUserCodeDependencies, pythonDependencyRepository,
                pythonExtraDependencyRepository, metricsPort, narExtractionDirectory));
        return cmd;
    }

    public static List<String> getArgsBeforeCmd(InstanceConfig instanceConfig, String extraDependenciesDir) {

        final List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            //no-op
        } else if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.PYTHON) {
            // add `extraDependenciesDir` to python package searching path
            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                args.add("PYTHONPATH=${PYTHONPATH}:" + extraDependenciesDir);
            }
        }

        return args;
    }

    /**
     * Different from python and java function, Go function uploads a complete executable file(including:
     * instance file + user code file). Its parameter list is provided to the broker in the form of a yaml file,
     * the advantage of this approach is that backward compatibility is guaranteed.
     *
     * In Java and Python the instance is managed by broker (or function worker) so the changes in command line
     * is under control; but in Go the instance is compiled with the user function, so pulsar doesn't have the
     * control what instance is used in the function. Hence in order to support BC for go function, we can't
     * dynamically add more commandline arguments. Using an instance config to pass the parameters from function
     * worker to go instance is the best way for maintaining the BC.
     * <p>
     * When we run the go function, we only need to specify the location of the go-function file and the yaml file.
     * The content of the yaml file will be automatically generated according to the content provided by instanceConfig.
     */

    public static List<String> getGoInstanceCmd(InstanceConfig instanceConfig,
                                                String originalCodeFileName,
                                                String pulsarServiceUrl) throws IOException {
        final List<String> args = new LinkedList<>();
        GoInstanceConfig goInstanceConfig = new GoInstanceConfig();

        if (instanceConfig.getClusterName() != null) {
            goInstanceConfig.setClusterName(instanceConfig.getClusterName());
        }

        if (instanceConfig.getInstanceId() != 0) {
            goInstanceConfig.setInstanceID(instanceConfig.getInstanceId());
        }

        if (instanceConfig.getFunctionId() != null) {
            goInstanceConfig.setFuncID(instanceConfig.getFunctionId());
        }

        if (instanceConfig.getFunctionVersion() != null) {
            goInstanceConfig.setFuncVersion(instanceConfig.getFunctionVersion());
        }

        if (instanceConfig.getFunctionDetails().getAutoAck()) {
            goInstanceConfig.setAutoAck(instanceConfig.getFunctionDetails().getAutoAck());
        }

        if (instanceConfig.getFunctionDetails().getTenant() != null) {
            goInstanceConfig.setTenant(instanceConfig.getFunctionDetails().getTenant());
        }

        if (instanceConfig.getFunctionDetails().getNamespace() != null) {
            goInstanceConfig.setNameSpace(instanceConfig.getFunctionDetails().getNamespace());
        }

        if (instanceConfig.getFunctionDetails().getName() != null) {
            goInstanceConfig.setName(instanceConfig.getFunctionDetails().getName());
        }

        if (instanceConfig.getFunctionDetails().getLogTopic() != null) {
            goInstanceConfig.setLogTopic(instanceConfig.getFunctionDetails().getLogTopic());
        }
        if (instanceConfig.getFunctionDetails().getProcessingGuarantees() != null) {
            goInstanceConfig.setProcessingGuarantees(instanceConfig.getFunctionDetails().getProcessingGuaranteesValue());
        }
        if (instanceConfig.getFunctionDetails().getSecretsMap() != null) {
            goInstanceConfig.setSecretsMap(instanceConfig.getFunctionDetails().getSecretsMap());
        }
        if (instanceConfig.getFunctionDetails().getParallelism() != 0) {
            goInstanceConfig.setParallelism(instanceConfig.getFunctionDetails().getParallelism());
        }

        if (instanceConfig.getMaxBufferedTuples() != 0) {
            goInstanceConfig.setMaxBufTuples(instanceConfig.getMaxBufferedTuples());
        }

        if (pulsarServiceUrl != null) {
            goInstanceConfig.setPulsarServiceURL(pulsarServiceUrl);
        }
        if (instanceConfig.getFunctionDetails().getSource().getCleanupSubscription()) {
            goInstanceConfig.setCleanupSubscription(instanceConfig.getFunctionDetails().getSource().getCleanupSubscription());
        }
        if (instanceConfig.getFunctionDetails().getSource().getSubscriptionName() != null) {
            goInstanceConfig.setSubscriptionName(instanceConfig.getFunctionDetails().getSource().getSubscriptionName());
        }

        if (instanceConfig.getFunctionDetails().getSource().getInputSpecsMap() != null) {
            for (String inputTopic : instanceConfig.getFunctionDetails().getSource().getInputSpecsMap().keySet()) {
                goInstanceConfig.setSourceSpecsTopic(inputTopic);
            }
        }

        if (instanceConfig.getFunctionDetails().getSource().getTimeoutMs() != 0) {
            goInstanceConfig.setTimeoutMs(instanceConfig.getFunctionDetails().getSource().getTimeoutMs());
        }

        if (instanceConfig.getFunctionDetails().getSink().getTopic() != null) {
            goInstanceConfig.setSinkSpecsTopic(instanceConfig.getFunctionDetails().getSink().getTopic());
        }

        if (instanceConfig.getFunctionDetails().getResources().getCpu() != 0) {
            goInstanceConfig.setCpu(instanceConfig.getFunctionDetails().getResources().getCpu());
        }

        if (instanceConfig.getFunctionDetails().getResources().getRam() != 0) {
            goInstanceConfig.setRam(instanceConfig.getFunctionDetails().getResources().getRam());
        }

        if (instanceConfig.getFunctionDetails().getResources().getDisk() != 0) {
            goInstanceConfig.setDisk(instanceConfig.getFunctionDetails().getResources().getDisk());
        }

        if (instanceConfig.getFunctionDetails().getRetryDetails().getDeadLetterTopic() != null) {
            goInstanceConfig.setDeadLetterTopic(instanceConfig.getFunctionDetails().getRetryDetails().getDeadLetterTopic());
        }

        if (instanceConfig.getFunctionDetails().getRetryDetails().getMaxMessageRetries() != 0) {
            goInstanceConfig.setMaxMessageRetries(instanceConfig.getFunctionDetails().getRetryDetails().getMaxMessageRetries());
        }

        goInstanceConfig.setKillAfterIdleMs(0);
        goInstanceConfig.setPort(instanceConfig.getPort());

        // Parse the contents of goInstanceConfig into json form string
        ObjectMapper objectMapper = ObjectMapperFactory.getThreadLocal();
        String configContent = objectMapper.writeValueAsString(goInstanceConfig);

        // Nit: at present, the implementation of go function depends on pulsar-client-go,
        // pulsar-client-go uses cgo, so the currently uploaded executable doesn't support cross-compilation.
        args.add(originalCodeFileName);
        args.add("-instance-conf");
        args.add(configContent);
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
                                      int metricsPort,
                                      String narExtractionDirectory) throws Exception {
        final List<String> args = new LinkedList<>();

        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.GO) {
            return getGoInstanceCmd(instanceConfig, originalCodeFileName, pulsarServiceUrl);
        }

        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");

            String classpath = instanceFile;

            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                classpath = classpath + ":" + extraDependenciesDir + "/*";
            }
            args.add(classpath);

            if (StringUtils.isNotEmpty(extraDependenciesDir)) {
                args.add(String.format("-D%s=%s", FUNCTIONS_EXTRA_DEPS_PROPERTY, extraDependenciesDir));
            }

            // add complete classpath for broker/worker so that the function instance can load
            // the functions instance dependencies separately from user code dependencies
            String functionInstanceClasspath = System.getProperty(FUNCTIONS_INSTANCE_CLASSPATH);
            if (functionInstanceClasspath == null) {
                log.warn("Property {} is not set.  Falling back to using classpath of current JVM", FUNCTIONS_INSTANCE_CLASSPATH);
                functionInstanceClasspath = System.getProperty("java.class.path");
            }
            args.add(String.format("-D%s=%s", FUNCTIONS_INSTANCE_CLASSPATH, functionInstanceClasspath));

            args.add("-Dlog4j.configurationFile=" + logConfigFile);
            args.add("-Dpulsar.function.log.dir=" + genFunctionLogFolder(logDirectory, instanceConfig));
            args.add("-Dpulsar.function.log.file=" + String.format(
                    "%s-%s",
                    instanceConfig.getFunctionDetails().getName(),
                    shardId));
            if (!isEmpty(instanceConfig.getFunctionDetails().getRuntimeFlags())) {
                for (String runtimeFlagArg : splitRuntimeArgs(instanceConfig.getFunctionDetails().getRuntimeFlags())) {
                    args.add(runtimeFlagArg);
                }
            }
            if (instanceConfig.getFunctionDetails().getResources() != null) {
                Function.Resources resources = instanceConfig.getFunctionDetails().getResources();
                if (resources.getRam() != 0) {
                    args.add("-Xmx" + String.valueOf(resources.getRam()));
                }
            }
            args.add("org.apache.pulsar.functions.instance.JavaInstanceMain");

            args.add("--jar");
            args.add(originalCodeFileName);
        } else if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.PYTHON) {
            args.add("python");
            if (!isEmpty(instanceConfig.getFunctionDetails().getRuntimeFlags())) {
                for (String runtimeFlagArg : splitRuntimeArgs(instanceConfig.getFunctionDetails().getRuntimeFlags())) {
                    args.add(runtimeFlagArg);
                }
            }
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

        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            if (!StringUtils.isEmpty(narExtractionDirectory)) {
                args.add("--nar_extraction_directory");
                args.add(narExtractionDirectory);
            }
        }
        return args;
    }

    public static String genFunctionLogFolder(String logDirectory, InstanceConfig instanceConfig) {
        return String.format(
                "%s/%s",
                logDirectory,
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
    }

    public static String getPrometheusMetrics(int metricsPort) throws IOException {
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

    /**
     * Regex for splitting a string using space when not surrounded by single or double quotes
     */
    public static String[] splitRuntimeArgs(String input) {
        return input.split("\\s(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    public static <T> T getRuntimeFunctionConfig(Map<String, Object> configMap, Class<T> functionRuntimeConfigClass) {
        return ObjectMapperFactory.getThreadLocal().convertValue(configMap, functionRuntimeConfigClass);
    }

}
