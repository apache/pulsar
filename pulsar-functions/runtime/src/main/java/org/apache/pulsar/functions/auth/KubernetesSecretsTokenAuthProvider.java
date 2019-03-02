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
package org.apache.pulsar.functions.auth;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1ObjectReference;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1Secret;
import io.kubernetes.client.models.V1SecretVolumeSource;
import io.kubernetes.client.models.V1ServiceAccount;
import io.kubernetes.client.models.V1StatefulSet;
import io.kubernetes.client.models.V1Volume;
import io.kubernetes.client.models.V1VolumeMount;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.RuntimeUtils;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;

import javax.naming.AuthenticationException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.pulsar.broker.authentication.AuthenticationProviderToken.HTTP_HEADER_VALUE_PREFIX;
import static org.apache.pulsar.client.impl.auth.AuthenticationDataToken.HTTP_HEADER_NAME;

@Slf4j
public class KubernetesSecretsTokenAuthProvider implements KubernetesFunctionAuthProvider {

    private static final int NUM_RETRIES = 5;
    private static final long SLEEP_BETWEEN_RETRIES_MS = 500;
    private static final String SECRET_NAME = "function-auth";
    private static final String DEFAULT_SECRET_MOUNT_DIR = "/etc/auth";
    private static final String FUNCTION_AUTH_TOKEN = "token";


    private final CoreV1Api coreClient;
    private final String kubeNamespace;

    public KubernetesSecretsTokenAuthProvider(CoreV1Api coreClient, String kubeNamespace) {
        this.coreClient = coreClient;
        this.kubeNamespace = kubeNamespace;
    }

    @Override
    public void configureAuthDataStatefulSet(Function.FunctionAuthenticationSpec functionAuthenticationSpec, V1StatefulSet statefulSet) {

        V1PodSpec podSpec = statefulSet.getSpec().getTemplate().getSpec();

        // configure pod mount secret with auth token
        Function.FunctionAuthenticationSpec authenticationSpec = functionAuthenticationSpec;
        podSpec.setVolumes(Collections.singletonList(
                new V1Volume()
                        .name(SECRET_NAME)
                        .secret(
                                new V1SecretVolumeSource()
                                        .secretName(getSecretName(authenticationSpec.getData()))
                                        .defaultMode(256))));

        podSpec.getContainers().forEach(container -> container.setVolumeMounts(Collections.singletonList(
                new V1VolumeMount()
                        .name(SECRET_NAME)
                        .mountPath(DEFAULT_SECRET_MOUNT_DIR)
                        .readOnly(true))));

    }

    @Override
    public void configureAuthenticationConfig(AuthenticationConfig authConfig, Function.FunctionAuthenticationSpec functionAuthenticationSpec) {
        authConfig.setClientAuthenticationPlugin(AuthenticationToken.class.getName());
        authConfig.setClientAuthenticationParameters(String.format("file://%s/%s", DEFAULT_SECRET_MOUNT_DIR, FUNCTION_AUTH_TOKEN));
    }

    @Override
    public void configureAuthDataKubernetesServiceAccount(Function.FunctionAuthenticationSpec functionAuthenticationSpec, V1ServiceAccount sa) {
       sa.addSecretsItem(new V1ObjectReference().name("pf-secret-" + functionAuthenticationSpec.getData()).namespace(kubeNamespace));
    }

    @Override
    public Function.FunctionAuthenticationSpec cacheAuthData(String tenant, String namespace, String name,
                                                             AuthenticationDataSource authenticationDataSource) {
        String id = null;
        try {
            String token = getToken(authenticationDataSource);
            if (token != null) {
                id = createSecret(token, tenant, namespace, name);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (id != null) {
            return Function.FunctionAuthenticationSpec.newBuilder().setData(id).build();
        }
        return null;
    }

    @Override
    public void cleanUpAuthData(String tenant, String namespace, String name, Function.FunctionAuthenticationSpec
            functionAuthenticationSpec) throws Exception {
        String fqfn = FunctionDetailsUtils.getFullyQualifiedName(tenant, namespace, name);

        String secretName = functionAuthenticationSpec.getData();
        RuntimeUtils.Actions.Action deleteSecrets = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Deleting secrets for function %s", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    try {
                        V1DeleteOptions v1DeleteOptions = new V1DeleteOptions();
                        v1DeleteOptions.setGracePeriodSeconds(0L);
                        v1DeleteOptions.setPropagationPolicy("Foreground");

                        coreClient.deleteNamespacedSecret(secretName,
                                kubeNamespace, v1DeleteOptions, "true",
                                null, null, null);
                    } catch (ApiException e) {
                        // if already deleted
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            log.warn("Secrets for function {} does not exist", fqfn);
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }
                    return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                })
                .build();

        RuntimeUtils.Actions.Action waitForSecretsDeletion = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Waiting for secrets for function %s to complete deletion", fqfn))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    try {
                        coreClient.readNamespacedSecret(secretName, kubeNamespace,
                                null, null, null);

                    } catch (ApiException e) {
                        // statefulset is gone
                        if (e.getCode() == HTTP_NOT_FOUND) {
                            return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                        }
                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }
                    return RuntimeUtils.Actions.ActionResult.builder()
                            .success(false)
                            .build();
                })
                .build();

        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(deleteSecrets.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForSecretsDeletion.toBuilder()
                        .continueOn(false)
                        .onSuccess(() -> success.set(true))
                        .build())
                .addAction(deleteSecrets.toBuilder()
                        .continueOn(true)
                        .build())
                .addAction(waitForSecretsDeletion.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to delete secrets for function %s", fqfn));
        }
    }

    private String createSecret(String token, String tenant, String namespace, String name) throws ApiException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        RuntimeUtils.Actions.Action createAuthSecret = RuntimeUtils.Actions.Action.builder()
                .actionName(String.format("Creating authentication secret for function %s/%s/%s", tenant, namespace, name))
                .numRetries(NUM_RETRIES)
                .sleepBetweenInvocationsMs(SLEEP_BETWEEN_RETRIES_MS)
                .supplier(() -> {
                    String id =  RandomStringUtils.random(5, true, true).toLowerCase();
                    V1Secret v1Secret = new V1Secret()
                            .metadata(new V1ObjectMeta().name(getSecretName(id)))
                            .data(Collections.singletonMap(FUNCTION_AUTH_TOKEN, token.getBytes()));
                    try {
                        coreClient.createNamespacedSecret(kubeNamespace, v1Secret, "true");
                    } catch (ApiException e) {
                        // already exists
                        if (e.getCode() == HTTP_CONFLICT) {
                            return RuntimeUtils.Actions.ActionResult.builder()
                                    .errorMsg(String.format("Secret %s already present", id))
                                    .success(false)
                                    .build();
                        }

                        String errorMsg = e.getResponseBody() != null ? e.getResponseBody() : e.getMessage();
                        return RuntimeUtils.Actions.ActionResult.builder()
                                .success(false)
                                .errorMsg(errorMsg)
                                .build();
                    }

                    sb.append(id.toCharArray());
                    return RuntimeUtils.Actions.ActionResult.builder().success(true).build();
                })
                .build();

        AtomicBoolean success = new AtomicBoolean(false);
        RuntimeUtils.Actions.newBuilder()
                .addAction(createAuthSecret.toBuilder()
                        .onSuccess(() -> success.set(true))
                        .build())
                .run();

        if (!success.get()) {
            throw new RuntimeException(String.format("Failed to create authentication secret for function %s/%s/%s", tenant, namespace, name));
        }

        return sb.toString();
    }

    private String getSecretName(String id) {
        return "pf-secret-" + id;
    }

    private String getToken(AuthenticationDataSource authData) throws AuthenticationException {
        if (authData.hasDataFromCommand()) {
            // Authenticate Pulsar binary connection
            return authData.getCommandData();
        } else if (authData.hasDataFromHttp()) {
            // Authentication HTTP request. The format here should be compliant to RFC-6750
            // (https://tools.ietf.org/html/rfc6750#section-2.1). Eg: Authorization: Bearer xxxxxxxxxxxxx
            String httpHeaderValue = authData.getHttpHeader(HTTP_HEADER_NAME);
            if (httpHeaderValue == null || !httpHeaderValue.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                throw new AuthenticationException("Invalid HTTP Authorization header");
            }

            // Remove prefix
            String token = httpHeaderValue.substring(HTTP_HEADER_VALUE_PREFIX.length());
            return validateToken(token);
        } else {
            return null;
        }
    }

    private String validateToken(final String token) throws AuthenticationException {
        if (StringUtils.isNotBlank(token)) {
            return token;
        } else {
            throw new AuthenticationException("Blank token found");
        }
    }
}
