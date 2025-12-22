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
package org.apache.pulsar.tests.integration.functions.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import io.kubernetes.client.Exec;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.tests.integration.functions.java.PulsarFunctionsJavaTest;
import org.apache.pulsar.tests.integration.functions.utils.CommandGenerator;
import org.apache.pulsar.tests.integration.k8s.AbstractPulsarStandaloneK8STest;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * This class is an integration test for Pulsar Functions running in Kubernetes.
 * This tests {@link org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory},
 * {@link org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntime},
 * and {@link org.apache.pulsar.functions.secretsproviderconfigurator.KubernetesSecretsProviderConfigurator} classes
 * in a lightweight Kubernetes cluster which is provided by a k3s container running in Docker with Testcontainers.
 */
@Slf4j
public class PulsarFunctionsK8STest extends AbstractPulsarStandaloneK8STest {
    @Test
    public void testCreateFunctionInK8sWithSecrets()
            throws PulsarAdminException, IOException, InterruptedException, ApiException {
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getPulsarWebServiceUrl())
                .build();

        String randomPart = UUID.randomUUID().toString();
        String inputTopicName =
                "persistent://public/default/test-function-input-" + randomPart;
        String outputTopicName = "persistent://public/default/test-function-output-" + randomPart;

        // create a sample secret
        try {
            CoreV1Api api = new CoreV1Api(getApiClient());
            V1Secret secret = new V1SecretBuilder()
                    .withNewMetadata()
                    .withName("mysecret")
                    .endMetadata()
                    .addToStringData("secret1", "value1")
                    .addToStringData("secret2", "value2").build();
            api.createNamespacedSecret(getNamespace(), secret).execute();
        } catch (ApiException e) {
            // ignore if the secret already exists since this could happen due to retries
            if (e.getCode() != 409) {
                throw e;
            }
        }

        FunctionConfig functionConfig = new FunctionConfig();
        String fnTenant = "public";
        String fnNamespace = "default";
        String fnName = "test-function";
        functionConfig.setTenant(fnTenant);
        functionConfig.setNamespace(fnNamespace);
        functionConfig.setName(fnName);
        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        functionConfig.setClassName(PulsarFunctionsJavaTest.EXCLAMATION_JAVA_CLASS);
        functionConfig.setJar(CommandGenerator.JAVAJAR);
        functionConfig.setInputs(List.of(inputTopicName));
        functionConfig.setOutput(outputTopicName);
        // test referencing k8s secrets
        functionConfig.setSecrets(
                Map.of("secret1", Map.of(
                                "path", "mysecret",
                                "key", "secret1"),
                        "secret2", Map.of(
                                "path", "mysecret",
                                "key", "secret2")));

        log.info("Creating function");
        admin.functions().createFunctionWithUrl(functionConfig, "file://" + CommandGenerator.JAVAJAR);

        log.info("Waiting for function to be created");
        Awaitility.await().ignoreExceptions().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
            FunctionStatus functionStatus = admin.functions().getFunctionStatus(fnTenant, fnNamespace, fnName);
            assertThat(functionStatus.getNumInstances()).isEqualTo(1);
            assertThat(functionStatus.getNumRunning()).isEqualTo(1);
        });
        log.info("Function created successfully");

        log.info("Waiting for function to subscribe to input topic");
        Awaitility.await().ignoreExceptions().atMost(Duration.ofSeconds(30))
                .until(() -> {
                    admin.topics().getSubscriptions(inputTopicName);
                    return true;
                });
        log.info("Function subscribed to input topic");

        // Validate that k8s secrets were provided as environment variables to the function pod
        String podName = "pf-%s-%s-%s-0".formatted(fnTenant, fnNamespace, fnName);
        Exec exec = new Exec(getApiClient());
        Process process = exec.newExecutionBuilder(getNamespace(), podName, new String[]{"sh", "-c",
                        "echo \"secret1=$secret1,secret2=$secret2\""})
                .setStdin(false).setStderr(false).setStdout(true).setTty(false).execute();
        String response = process.inputReader().readLine();
        assertThat(process.waitFor()).isEqualTo(0);
        assertThat(response).isEqualTo("secret1=value1,secret2=value2");

        log.info("Testing function");
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(getPulsarBrokerUrl())
                .build();

        @Cleanup
        Consumer<String> consumer =
                client.newConsumer(Schema.STRING).topic(outputTopicName).subscriptionName("sub").subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING).topic(inputTopicName).create();
        producer.send("Hello");

        Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getValue()).isEqualTo("Hello!");


        log.info("Stopping function");
        admin.functions().stopFunction(fnTenant, fnNamespace, fnName);
        Awaitility.await().ignoreExceptions().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    FunctionStatus functionStatus = admin.functions().getFunctionStatus(fnTenant, fnNamespace, fnName);
                    assertThat(functionStatus.getNumInstances()).isEqualTo(1);
                    assertThat(functionStatus.getNumRunning()).isEqualTo(0);
                });

        log.info("Starting function");
        // this seems to be flaky if the stopping of the function hasn't fully completed when it's started again.
        // one way to reproduce is to remove the delay before starting the function and also removing the pollDelay
        // from the await after stopFunction
        Thread.sleep(2000);
        admin.functions().startFunction(fnTenant, fnNamespace, fnName);
        Awaitility.await().ignoreExceptions().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    FunctionStatus functionStatus = admin.functions().getFunctionStatus(fnTenant, fnNamespace, fnName);
                    assertThat(functionStatus.getNumInstances()).isEqualTo(1);
                    assertThat(functionStatus.getNumRunning()).isEqualTo(1);
                });

        log.info("Waiting for function to be deleted");
        admin.functions().deleteFunction(fnTenant, fnNamespace, fnName);
        log.info("Function deleted successfully");

        log.info("Waiting for function pod to be deleted");
        CoreV1Api coreApi = new CoreV1Api(getApiClient());
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThatExceptionOfType(ApiException.class).isThrownBy(
                    () -> coreApi.readNamespacedPod(getNamespace(), podName).execute())
                    .satisfies(apiException -> {
                assertThat(apiException.getCode()).isEqualTo(404);
            });
        });
        log.info("Function pod deleted successfully");
    }
}
