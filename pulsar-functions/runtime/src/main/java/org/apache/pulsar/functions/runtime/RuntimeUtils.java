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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheEntry;

import java.util.*;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Util class for common runtime functionality
 */
@Slf4j
class RuntimeUtils {

    public static List<String> composeArgs(InstanceConfig instanceConfig,
                                           String instanceFile,
                                           String logDirectory,
                                           String originalCodeFileName,
                                           String pulsarServiceUrl,
                                           String stateStorageServiceUrl,
                                           AuthenticationConfig authConfig,
                                           String shardId,
                                           Integer grpcPort,
                                           Long expectedHealthCheckInterval,
                                           String javaLog4jFileName,
                                           Boolean installUserCodeDepdendencies) throws Exception {
        List<String> args = new LinkedList<>();
        if (instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            args.add("java");
            args.add("-cp");
            args.add(instanceFile);

            // Keep the same env property pointing to the Java instance file so that it can be picked up
            // by the child process and manually added to classpath
            args.add(String.format("-D%s=%s", FunctionCacheEntry.JAVA_INSTANCE_JAR_PROPERTY, instanceFile));
            args.add("-Dlog4j.configurationFile=" + javaLog4jFileName);
            args.add("-Dpulsar.function.log.dir=" + String.format(
                    "%s/%s",
                    logDirectory,
                    FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails())));
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
            // `installUserCodeDependencies` is only valid for python runtime
            if (installUserCodeDepdendencies != null && installUserCodeDepdendencies) {
                args.add("--install_usercode_dependencies");
                args.add("True");
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

        // state storage configs
        if (null != stateStorageServiceUrl
                && instanceConfig.getFunctionDetails().getRuntime() == Function.FunctionDetails.Runtime.JAVA) {
            args.add("--state_storage_serviceurl");
            args.add(stateStorageServiceUrl);
        }
        args.add("--expected_healthcheck_interval");
        args.add(String.valueOf(expectedHealthCheckInterval));
        return args;
    }
}
