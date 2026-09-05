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
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.slf4j.Logger;

/**
 * Utility for test clients.
 */
@Slf4j
@UtilityClass
public class PerfClientUtils {

    /**
     * Number of significant decimal digits kept by the perf clients' latency histograms.
     *
     * <p>HdrHistogram sizes a fixed-range histogram's counts array proportionally to
     * {@code 2^ceil(log2(2 * 10^digits))}, so every extra digit multiplies the allocation by
     * roughly 10. At 5 digits (HdrHistogram's maximum) a single {@code Recorder} over the
     * ranges used here costs 11-22 MB, and since these recorders are static fields the JVM
     * pays for every perf subcommand, not just the one being run.
     *
     * <p>3 digits bounds the error of a reported percentile at 0.1%. That is far below the
     * run-to-run variance of a benchmark, and finer than the reports resolve anyway: the
     * microsecond-based tools print milliseconds with {@code %.3f}, and the millisecond-based
     * ones print whole milliseconds with {@code %d}. It is also HdrHistogram's own recommended
     * default.
     */
    public static final int LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS = 3;

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
        log.info("JVM args {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
        log.info("Netty max memory (PlatformDependent.maxDirectMemory()) {}",
                FileUtils.byteCountToDisplaySize(DirectMemoryUtils.jvmMaxDirectMemory()));
        log.info("JVM max heap memory (Runtime.getRuntime().maxMemory()) {}",
                FileUtils.byteCountToDisplaySize(Runtime.getRuntime().maxMemory()));
    }

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

        if (isNotBlank(arguments.sslfactoryPlugin)) {
            clientBuilder.sslFactoryPlugin(arguments.sslfactoryPlugin)
                    .sslFactoryPluginParams(arguments.sslFactoryPluginParams);
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

    public static PulsarAdminBuilder createAdminBuilderFromArguments(PerformanceBaseArguments arguments,
                                                                     final String adminUrl)
            throws PulsarClientException.UnsupportedAuthenticationException {

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl)
                .tlsTrustCertsFilePath(arguments.tlsTrustCertsFilePath);

        if (isNotBlank(arguments.authPluginClassName)) {
            pulsarAdminBuilder.authentication(arguments.authPluginClassName, arguments.authParams);
        }

        if (isNotBlank(arguments.sslfactoryPlugin)) {
            pulsarAdminBuilder.sslFactoryPlugin(arguments.sslfactoryPlugin)
                    .sslFactoryPluginParams(arguments.sslFactoryPluginParams);
        }

        if (arguments.tlsAllowInsecureConnection != null) {
            pulsarAdminBuilder.allowTlsInsecureConnection(arguments.tlsAllowInsecureConnection);
        }

        if (arguments.tlsHostnameVerificationEnable != null) {
            pulsarAdminBuilder.enableTlsHostnameVerification(arguments.tlsHostnameVerificationEnable);
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
            log.error("Failed to close client", e);
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
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
