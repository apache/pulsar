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
package org.apache.pulsar.client.impl.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.AuthenticationHttpClient;
import org.apache.pulsar.client.api.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

public class DefaultAuthenticationHttpClientFactoryTest {

    @Test
    public void createFallsBackToDefaultSslContextWhenTrustCertFileCannotBeLoaded() throws Exception {
        Path invalidTrustCertFile = Files.createTempFile("invalid-trust-cert", ".pem");
        Files.write(invalidTrustCertFile, "not a certificate".getBytes(StandardCharsets.UTF_8));

        try (MockedConstruction<DefaultAsyncHttpClient> mockedHttpClient =
                     mockConstruction(DefaultAsyncHttpClient.class)) {
            DefaultAuthenticationHttpClientFactory factory =
                    new DefaultAuthenticationHttpClientFactory(null, null, null);

            AuthenticationHttpClient client = factory.create(AuthenticationHttpClientConfig.builder()
                    .trustCertsFilePath(invalidTrustCertFile.toString())
                    .build());

            assertThat(client).isNotNull();
            assertThat(mockedHttpClient.constructed()).hasSize(1);
            client.close();
        }
    }

    @Test
    public void createClosesSslResourcesWhenHttpClientConstructionFails() throws Exception {
        ScheduledExecutorService sslRefreshScheduler = org.mockito.Mockito.mock(ScheduledExecutorService.class);
        ScheduledFuture<?> scheduledFuture = org.mockito.Mockito.mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(sslRefreshScheduler)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.SECONDS));

        try (MockedStatic<Executors> mockedExecutors = mockStatic(Executors.class);
             MockedConstruction<DefaultPulsarSslFactory> mockedSslFactory =
                     mockConstruction(DefaultPulsarSslFactory.class, (mock, context) -> { });
             MockedConstruction<DefaultAsyncHttpClient> ignoredHttpClient =
                     mockConstruction(DefaultAsyncHttpClient.class, (mock, context) -> {
                         throw new RuntimeException("AHC construction failed");
                     })) {
            mockedExecutors.when(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class)))
                    .thenReturn(sslRefreshScheduler);
            DefaultAuthenticationHttpClientFactory factory =
                    new DefaultAuthenticationHttpClientFactory(null, null, null);

            assertThrows(PulsarClientException.InvalidConfigurationException.class, () ->
                    factory.create(AuthenticationHttpClientConfig.builder()
                            .tlsCertificateFilePath("client-cert.pem")
                            .tlsKeyFilePath("client-key.pem")
                            .autoCertRefreshDuration(Duration.ofSeconds(1))
                            .build()));

            DefaultPulsarSslFactory sslFactory = mockedSslFactory.constructed().get(0);
            verify(sslRefreshScheduler).shutdownNow();
            verify(sslFactory).close();
        }
    }
}
