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
package org.apache.pulsar.functions.instance;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import lombok.experimental.UtilityClass;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.common.util.Reflections;

import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.functions.utils.FunctionCommon;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@UtilityClass
public class InstanceUtils {
    public static SerDe<?> initializeSerDe(String serdeClassName, ClassLoader clsLoader, Class<?> typeArg,
                                           boolean deser) {
        SerDe<?> serDe = createInstance(serdeClassName, clsLoader, SerDe.class);

        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
        if (deser) {
            checkArgument(typeArg.isAssignableFrom(inputSerdeTypeArgs[0]),
                    "Inconsistent types found between function input type and serde type: "
                            + " function type = " + typeArg + " should be assignable from "
                            + inputSerdeTypeArgs[0]);
        } else {
            checkArgument(inputSerdeTypeArgs[0].isAssignableFrom(typeArg),
                    "Inconsistent types found between function input type and serde type: "
                            + " serde type = " + inputSerdeTypeArgs[0] + " should be assignable from "
                            + typeArg);
        }

        return serDe;
    }

    public static Schema<?> initializeCustomSchema(String schemaClassName, ClassLoader clsLoader, Class<?> typeArg,
                                                   boolean input) {
        Schema<?> schema = createInstance(schemaClassName, clsLoader, Schema.class);

        Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(Schema.class, schema.getClass());
        if (input) {
            checkArgument(typeArg.isAssignableFrom(inputSerdeTypeArgs[0]),
                    "Inconsistent types found between function type and schema type: "
                            + " function type = " + typeArg + " should be assignable from "
                            + inputSerdeTypeArgs[0]);
        } else {
            checkArgument(inputSerdeTypeArgs[0].isAssignableFrom(typeArg),
                    "Inconsistent types found between function type and schema type: "
                            + " schema type = " + inputSerdeTypeArgs[0] + " should be assignable from "
                            + typeArg);
        }
        return schema;
    }

    private static <T> T createInstance(String className, ClassLoader clsLoader, Class<T> baseClass) {
        if (StringUtils.isEmpty(className)) {
            return null;
        } else {
            return Reflections.createInstance(className, baseClass, clsLoader);
        }
    }

    public Function.FunctionDetails.ComponentType calculateSubjectType(Function.FunctionDetails functionDetails) {
        if (functionDetails.getComponentType() != Function.FunctionDetails.ComponentType.UNKNOWN) {
            return functionDetails.getComponentType();
        }
        Function.SourceSpec sourceSpec = functionDetails.getSource();
        Function.SinkSpec sinkSpec = functionDetails.getSink();
        if (sourceSpec.getInputSpecsCount() == 0) {
            return Function.FunctionDetails.ComponentType.SOURCE;
        }
        // Now its between sink and function

        if (!isEmpty(sinkSpec.getBuiltin())) {
            // if its built in, its a sink
            return Function.FunctionDetails.ComponentType.SINK;
        }

        if (isEmpty(sinkSpec.getClassName()) || sinkSpec.getClassName().equals(PulsarSink.class.getName())) {
            return Function.FunctionDetails.ComponentType.FUNCTION;
        }
        return Function.FunctionDetails.ComponentType.SINK;
    }

    public static String getDefaultSubscriptionName(String tenant, String namespace, String name) {
        return FunctionCommon.getFullyQualifiedName(tenant, namespace, name);
    }

    public static String getDefaultSubscriptionName(Function.FunctionDetails functionDetails) {
        return getDefaultSubscriptionName(
                functionDetails.getTenant(),
                functionDetails.getNamespace(),
                functionDetails.getName());
    }

    public static Map<String, String> getProperties(Function.FunctionDetails.ComponentType componentType,
                                                    String fullyQualifiedName, int instanceId) {
        Map<String, String> properties = new HashMap<>();
        switch (componentType) {
            case FUNCTION:
                properties.put("application", "pulsar-function");
                break;
            case SOURCE:
                properties.put("application", "pulsar-source");
                break;
            case SINK:
                properties.put("application", "pulsar-sink");
                break;
            default:
                throw new IllegalArgumentException("Not support component type");
        }
        properties.put("id", fullyQualifiedName);
        properties.put("instance_id", String.valueOf(instanceId));
        try {
            properties.put("instance_hostname", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            log.warn("[{}:{}] Failed to get hostname of instance", fullyQualifiedName, instanceId, e);
        }
        return properties;
    }

    public static ClientBuilder createPulsarClientBuilder(String pulsarServiceUrl,
                                                          AuthenticationConfig authConfig,
                                                          Optional<Long> memoryLimit) throws PulsarClientException {
        ClientBuilder clientBuilder = null;
        if (isNotBlank(pulsarServiceUrl)) {
            clientBuilder = PulsarClient.builder()
                    .memoryLimit(0, SizeUnit.BYTES)
                    .serviceUrl(pulsarServiceUrl);
            if (authConfig != null) {
                if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                        && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                    clientBuilder.authentication(authConfig.getClientAuthenticationPlugin(),
                            authConfig.getClientAuthenticationParameters());
                }
                clientBuilder.enableTls(authConfig.isUseTls());
                clientBuilder.allowTlsInsecureConnection(authConfig.isTlsAllowInsecureConnection());
                clientBuilder.enableTlsHostnameVerification(authConfig.isTlsHostnameVerificationEnable());
                clientBuilder.tlsTrustCertsFilePath(authConfig.getTlsTrustCertsFilePath());
            }
            if (memoryLimit.isPresent()) {
                clientBuilder.memoryLimit(memoryLimit.get(), SizeUnit.BYTES);
            }
            clientBuilder.ioThreads(Runtime.getRuntime().availableProcessors());
            return clientBuilder;
        }
        throw new PulsarClientException("pulsarServiceUrl cannot be null");
    }

    public static PulsarClient createPulsarClient(String pulsarServiceUrl, AuthenticationConfig authConfig)
            throws PulsarClientException {
        return createPulsarClient(pulsarServiceUrl, authConfig, Optional.empty());
    }

    public static PulsarClient createPulsarClient(String pulsarServiceUrl,
                                                  AuthenticationConfig authConfig,
                                                  Optional<Long> memoryLimit) throws PulsarClientException {
        return createPulsarClientBuilder(pulsarServiceUrl, authConfig, memoryLimit).build();
    }

    public static PulsarAdmin createPulsarAdminClient(String pulsarWebServiceUrl, AuthenticationConfig authConfig)
            throws PulsarClientException {
        PulsarAdminBuilder pulsarAdminBuilder = null;
        if (isNotBlank(pulsarWebServiceUrl)) {
            pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(pulsarWebServiceUrl);
            if (authConfig != null) {
                if (isNotBlank(authConfig.getClientAuthenticationPlugin())
                        && isNotBlank(authConfig.getClientAuthenticationParameters())) {
                    pulsarAdminBuilder.authentication(authConfig.getClientAuthenticationPlugin(),
                            authConfig.getClientAuthenticationParameters());
                }
                if (isNotBlank(authConfig.getTlsTrustCertsFilePath())) {
                    pulsarAdminBuilder.tlsTrustCertsFilePath(authConfig.getTlsTrustCertsFilePath());
                }
                pulsarAdminBuilder.allowTlsInsecureConnection(authConfig.isTlsAllowInsecureConnection());
                pulsarAdminBuilder.enableTlsHostnameVerification(authConfig.isTlsHostnameVerificationEnable());
            }
            return pulsarAdminBuilder.build();
        }
        throw new PulsarClientException("pulsarWebServiceUrl cannot be null");
    }
}
