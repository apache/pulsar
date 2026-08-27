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
package org.apache.pulsar.testclient;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import io.github.merlimat.slog.Logger;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.ProxyProtocol;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.apache.pulsar.tls.TlsPolicy;

/**
 * Utility for test clients.
 */
@CustomLog
@UtilityClass
public class PerfClientUtils {

    private static volatile  Consumer<Integer> exitProcedure = System::exit;

    public static void setExitProcedure(Consumer<Integer> exitProcedure) {
        PerfClientUtils.exitProcedure = Objects.requireNonNull(exitProcedure);
    }

    public static void exit(int code) {
        exitProcedure.accept(code);
    }

    /**
     * Print useful JVM information, you need this information in order to be able
     * to compare the results of executions in different environments.
     * @param log
     */
    public static void printJVMInformation(Logger log) {
        log.info().attr("args", ManagementFactory.getRuntimeMXBean().getInputArguments()).log("JVM args");
        log.info()
                .attr("maxDirectMemory", FileUtils.byteCountToDisplaySize(DirectMemoryUtils.jvmMaxDirectMemory()))
                .log("Netty max memory (PlatformDependent.maxDirectMemory");
        log.info()
                .attr("maxMemory", FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()))
                .log("JVM max heap memory (Runtime.getRuntime.maxMemory");
    }

    @SuppressWarnings("deprecation")
    public static ClientBuilder createClientBuilderFromArguments(PerformanceBaseArguments arguments)
            throws PulsarClientException.UnsupportedAuthenticationException {

        ClientBuilder clientBuilder = PulsarClient.builder()
                .memoryLimit(arguments.memoryLimit, SizeUnit.BYTES)
                .serviceUrl(arguments.serviceURL)
                .connectionsPerBroker(arguments.maxConnections)
                .ioThreads(arguments.ioThreads)
                .statsInterval(arguments.statsIntervalSeconds, TimeUnit.SECONDS)
                .enableBusyWait(arguments.enableBusyWait)
                .listenerThreads(arguments.listenerThreads)
                .tlsTrustCertsFilePath(arguments.tlsTrustCertsFilePath)
                .maxLookupRequests(arguments.maxLookupRequest)
                .proxyServiceUrl(arguments.proxyServiceURL, arguments.proxyProtocol)
                .openTelemetry(AutoConfiguredOpenTelemetrySdk.builder()
                        .addPropertiesSupplier(() -> Map.of(
                                "otel.sdk.disabled", "true"
                        ))
                        .build().getOpenTelemetrySdk());

        if (isNotBlank(arguments.authPluginClassName)) {
            clientBuilder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        if (arguments.tlsAllowInsecureConnection != null) {
            clientBuilder.allowTlsInsecureConnection(arguments.tlsAllowInsecureConnection);
        }

        if (arguments.tlsHostnameVerificationEnable != null) {
            clientBuilder.enableTlsHostnameVerification(arguments.tlsHostnameVerificationEnable);
        }

        if (isNotBlank(arguments.listenerName)) {
            clientBuilder.listenerName(arguments.listenerName);
        }
        return clientBuilder;
    }

    /**
     * Build a V5 {@link PulsarClientBuilder} from the perf CLI arguments.
     *
     * <p>The V5 client is used by the perf commands so they work transparently against both
     * regular and scalable topics — the V5 SDK detects the topic kind via {@code topic://} vs
     * {@code persistent://} lookup and routes accordingly.
     *
     * <p>A few v4 settings have no direct V5 equivalent and are dropped here: {@code --stats-
     * interval-seconds} (V5 stats are OpenTelemetry-driven), {@code --max-lookup-request} (V5
     * does not expose a public knob), and {@code --busy-wait} (no V5 equivalent). All other
     * relevant flags map 1:1.
     */
    public static PulsarClientBuilder createV5ClientBuilderFromArguments(PerformanceBaseArguments arguments)
            throws org.apache.pulsar.client.api.v5.PulsarClientException {

        ConnectionPolicy.Builder connectionPolicy = ConnectionPolicy.builder()
                .connectionsPerBroker(arguments.maxConnections)
                .ioThreads(arguments.ioThreads)
                .callbackThreads(arguments.listenerThreads);
        if (isNotBlank(arguments.proxyServiceURL)) {
            ProxyProtocol v5Proto = arguments.proxyProtocol != null
                    ? ProxyProtocol.valueOf(arguments.proxyProtocol.name())
                    : null;
            connectionPolicy.proxy(arguments.proxyServiceURL, v5Proto);
        }

        PulsarClientBuilder builder = org.apache.pulsar.client.api.v5.PulsarClient.builder()
                .serviceUrl(arguments.serviceURL)
                .memoryLimit(MemorySize.ofBytes(arguments.memoryLimit))
                .connectionPolicy(connectionPolicy.build())
                .openTelemetry(AutoConfiguredOpenTelemetrySdk.builder()
                        .addPropertiesSupplier(() -> Map.of("otel.sdk.disabled", "true"))
                        .build().getOpenTelemetrySdk());

        if (isNotBlank(arguments.authPluginClassName)) {
            builder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        if (wantsTls(arguments)) {
            TlsPolicy.Builder tls = TlsPolicy.builder();
            if (isNotBlank(arguments.tlsTrustCertsFilePath)) {
                tls.trustCertsFilePath(arguments.tlsTrustCertsFilePath);
            }
            if (arguments.tlsAllowInsecureConnection != null) {
                tls.allowInsecureConnection(arguments.tlsAllowInsecureConnection);
            }
            if (arguments.tlsHostnameVerificationEnable != null) {
                tls.enableHostnameVerification(arguments.tlsHostnameVerificationEnable);
            }
            // PIP-478: both provider axes, so a FIPS run can pin BCJSSE and BCFIPS together. Format-
            // independent — they apply to PEM and keystore material alike.
            if (isNotBlank(arguments.jsseProvider)) {
                tls.jsseProvider(arguments.jsseProvider);
            }
            if (isNotBlank(arguments.jcaProvider)) {
                tls.jcaProvider(arguments.jcaProvider);
            }
            builder.tlsPolicy(tls.build());
        }

        if (isNotBlank(arguments.listenerName)) {
            builder.listenerName(arguments.listenerName);
        }

        return builder;
    }

    /**
     * Whether the arguments express an actual intent to use TLS, and so whether a {@code TlsPolicy} should be
     * wired onto the V5 builder at all — {@code PulsarClientBuilderV5#tlsPolicy} unconditionally flips
     * {@code useTls=true}, so setting one against a plaintext endpoint makes the client attempt a TLS
     * handshake the broker will close.
     *
     * <p>The Boolean flags arrive as {@code Boolean.FALSE} (not {@code null}) whenever picocli's
     * default-value resolution fires without the flag being passed, so "non-null" cannot mean "the user
     * wanted TLS". TLS is on when the URL is {@code pulsar+ssl://}, when a trust-cert path was supplied, or
     * when {@code tlsAllowInsecureConnection} was explicitly {@code TRUE}.
     *
     * <p>{@code tlsHostnameVerificationEnable} is deliberately <em>not</em> one of those signals. Hostname
     * verification is on by default since Pulsar 5.0 (PIP-478) and {@code conf/client.conf} ships that
     * default, so picocli's {@code descriptionKey} resolution hands us {@code TRUE} on every invocation in a
     * distribution, whether or not TLS was wanted. Reading it as intent forces a TLS handshake against a
     * plaintext {@code pulsar://} endpoint, which fails with "Connection closed while SSL/TLS handshake was
     * in progress". It still configures the policy once TLS is on for one of the reasons above.
     *
     * <p>Package-private for {@code PerfClientUtilsTest} (VisibleForTesting).
     *
     * @param arguments the parsed perf-tool arguments
     * @return whether a {@code TlsPolicy} should be configured
     */
    static boolean wantsTls(PerformanceBaseArguments arguments) {
        boolean tlsByUrl = arguments.serviceURL != null && arguments.serviceURL.startsWith("pulsar+ssl://");
        boolean tlsByTrustPath = isNotBlank(arguments.tlsTrustCertsFilePath);
        boolean tlsByAllowInsecure = Boolean.TRUE.equals(arguments.tlsAllowInsecureConnection);
        return tlsByUrl || tlsByTrustPath || tlsByAllowInsecure;
    }

    public static PulsarAdminBuilder createAdminBuilderFromArguments(PerformanceBaseArguments arguments,
                                                                     final String adminUrl)
            throws PulsarClientException.UnsupportedAuthenticationException {

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl)
                .tlsTrustCertsFilePath(arguments.tlsTrustCertsFilePath);

        if (isNotBlank(arguments.authPluginClassName)) {
            pulsarAdminBuilder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        if (arguments.tlsAllowInsecureConnection != null) {
            pulsarAdminBuilder.allowTlsInsecureConnection(arguments.tlsAllowInsecureConnection);
        }

        if (arguments.tlsHostnameVerificationEnable != null) {
            pulsarAdminBuilder.enableTlsHostnameVerification(arguments.tlsHostnameVerificationEnable);
        }

        // PIP-478: the admin leg must be pinned on the same two axes as the binary leg above, otherwise
        // `pulsar-perf --jsse-provider/--jca-provider` would parse the broker certificate for its HTTPS admin
        // calls through the JVM provider search order while the data connection is pinned — a FIPS-shaped run
        // rather than a FIPS one, on the tool whose flags exist to validate exactly that. PulsarAdminBuilder has
        // no fluent setter for either axis, so this mirrors BrokerService.configAdminTlsSettings and writes them
        // onto the underlying configuration.
        if (pulsarAdminBuilder instanceof PulsarAdminBuilderImpl adminBuilderImpl
                && (isNotBlank(arguments.jsseProvider) || isNotBlank(arguments.jcaProvider))) {
            ClientConfigurationData adminConf = adminBuilderImpl.getConf();
            if (isNotBlank(arguments.jsseProvider)) {
                adminConf.setJsseProvider(arguments.jsseProvider);
            }
            if (isNotBlank(arguments.jcaProvider)) {
                adminConf.setJcaProvider(arguments.jcaProvider);
            }
        }

        return pulsarAdminBuilder;
    }

    /**
     * This is used to register a shutdown hook that will be called when the JVM exits.
     * @param runnable the runnable to run on shutdown
     * @return the thread that was registered as a shutdown hook
     */
    public static Thread addShutdownHook(Runnable runnable) {
        Thread shutdownHookThread = new Thread(runnable, "perf-client-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        return shutdownHookThread;
    }

    /**
     * This is used to remove a previously registered shutdown hook and run it immediately.
     * This is useful at least for tests when there are multiple instances of the classes
     * in the JVM. It will also prevent resource leaks when test code isn't relying on the JVM
     * exit to clean up resources.
     * @param shutdownHookThread the shutdown hook thread to remove and run
     * @throws InterruptedException if the thread is interrupted while waiting for it to finish
     */
    public static void removeAndRunShutdownHook(Thread shutdownHookThread) throws InterruptedException {
        // clear interrupted status and restore later
        boolean wasInterrupted = Thread.currentThread().interrupted();
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            shutdownHookThread.start();
            shutdownHookThread.join();
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * This is used to close the client so that the interrupted status is cleared before
     * closing the client. This is needed if the thread is already interrupted before calling this method.
     * @param client the client to close
     */
    public static void closeClient(PulsarClient client) {
        if (client == null) {
            return;
        }
        // clear interrupted status so that the client can be shutdown
        boolean wasInterrupted = Thread.currentThread().interrupted();
        try {
            client.close();
        } catch (PulsarClientException e) {
            log.error().exception(e).log("Failed to close client");
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@link #closeClient(PulsarClient)} overload for the V5 client used by the perf tools. */
    public static void closeClient(org.apache.pulsar.client.api.v5.PulsarClient client) {
        if (client == null) {
            return;
        }
        boolean wasInterrupted = Thread.currentThread().interrupted();
        try {
            client.close();
        } catch (org.apache.pulsar.client.api.v5.PulsarClientException e) {
            log.error().exception(e).log("Failed to close client");
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Open a transaction on the V5 client, retrying briefly while the transaction-coordinator handler
     * finishes its asynchronous connect. The first {@code newTransaction()} right after the client is
     * built can race that connect and fail with {@code MetaStoreHandlerNotReadyException}; the perf
     * tools open their initial transaction before building producers/consumers, so they hit this
     * window (whereas a tool that builds participants first gives the handler time to connect).
     *
     * @param client the V5 client to open the transaction on
     * @return a new transaction once the coordinator is ready
     */
    public static org.apache.pulsar.client.api.v5.Transaction newTransactionWithRetry(
            org.apache.pulsar.client.api.v5.PulsarClient client)
            throws org.apache.pulsar.client.api.v5.PulsarClientException, InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (true) {
            try {
                return client.newTransaction();
            } catch (org.apache.pulsar.client.api.v5.PulsarClientException e) {
                if (System.currentTimeMillis() > deadline || hasInterruptedException(e)) {
                    throw e;
                }
                Thread.sleep(200);
            }
        }
    }

    /**
     * Check if the throwable or any of its causes is an InterruptedException.
     *
     * @param throwable the throwable to check
     * @return true if the throwable or any of its causes is an InterruptedException, false otherwise
     */
    public static boolean hasInterruptedException(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        if (throwable instanceof InterruptedException) {
            return true;
        }
        Throwable cause = throwable.getCause();
        while (cause != null) {
            if (cause instanceof InterruptedException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
