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
package org.apache.pulsar.broker.tls;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.security.Provider;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;

/**
 * Shared scaffolding for wiring a server component onto the PIP-478 {@link PulsarTlsFactory} SPI, which
 * replaces the PIP-337 {@code PulsarSslFactory} path (removed at the end of the PIP-478 series, leaving
 * this SPI the only server TLS path). Server components (broker,
 * proxy, websocket, functions-worker) call these helpers to instantiate and initialize the factory and parse
 * its parameters. A stale PIP-337 {@code sslFactoryPlugin} configuration key is rejected at config-file
 * load by {@code PulsarConfigurationLoader.rejectRemovedPip337TlsFactoryKeys}, so it never reaches these
 * helpers.
 *
 * <p>The helper deliberately does not touch Netty's {@code SslContext} itself — that subscribe pattern stays
 * inline in the binary-listener components — so it stays usable by every component, including the websocket
 * proxy and functions-worker web servers whose only TLS consumer is Jetty. It does reference
 * {@link SslProvider} and {@link OpenSsl} for the engine selection below; both come from
 * {@code netty-handler}, which reaches this module through its {@code pulsar-common} dependency.
 */
@CustomLog
public final class TlsFactorySupport {

    /**
     * Reserved {@code tlsFactoryClassName} value selecting the component's built-in default
     * {@link PulsarTlsFactory} (composed from the component configuration) via the new SPI, rather than a
     * reflectively-instantiated custom factory.
     */
    public static final String DEFAULT_FACTORY = "default";

    private TlsFactorySupport() {
    }

    /**
     * Instantiate the PIP-478 factory for the new path. A blank value, the literal {@link #DEFAULT_FACTORY},
     * or the default factory's own class name selects the supplied built-in default (composed from the
     * component configuration); any other value is instantiated reflectively via its public no-arg
     * constructor.
     *
     * @param tlsFactoryClassName the configured factory class name (or blank/{@code default})
     * @param defaultFactoryClass the class of the built-in default factory (for name matching); may be null
     * @param defaultFactory      supplies the built-in default factory
     * @return an uninitialized {@link PulsarTlsFactory} (call {@link #initializeBlocking} before use)
     * @throws ReflectiveOperationException if a named custom class cannot be instantiated
     */
    public static PulsarTlsFactory createFactory(String tlsFactoryClassName,
                                                 Class<? extends PulsarTlsFactory> defaultFactoryClass,
                                                 Supplier<PulsarTlsFactory> defaultFactory)
            throws ReflectiveOperationException {
        Objects.requireNonNull(defaultFactory, "defaultFactory must not be null");
        String className = tlsFactoryClassName == null ? "" : tlsFactoryClassName.trim();
        if (className.isEmpty()
                || DEFAULT_FACTORY.equalsIgnoreCase(className)
                || (defaultFactoryClass != null && defaultFactoryClass.getName().equals(className))) {
            return defaultFactory.get();
        }
        return (PulsarTlsFactory) Class.forName(className).getConstructor().newInstance();
    }

    /**
     * Build a production {@link TlsFactoryInitContext} with a no-op {@link OpenTelemetry}. For components
     * (websocket proxy, functions worker) whose module does not carry {@code opentelemetry-api} on its
     * compile classpath and only need TLS for the Jetty web path.
     *
     * @param params           the factory params (from {@link #parseFactoryConfig}); never null
     * @param scheduler        the framework scheduler for file-watch polling and rotation
     * @param blockingExecutor the executor for potentially-blocking material loading
     * @return a {@link TlsFactoryInitContext} with {@link OpenTelemetry#noop()}
     */
    public static TlsFactoryInitContext initContext(Map<String, String> params,
                                                    ScheduledExecutorService scheduler,
                                                    Executor blockingExecutor) {
        return initContext(params, scheduler, blockingExecutor, OpenTelemetry.noop());
    }

    /**
     * Build a production {@link TlsFactoryInitContext}.
     *
     * @param params           the factory params (from {@link #parseFactoryConfig}); never null
     * @param scheduler        the framework scheduler for file-watch polling and rotation
     * @param blockingExecutor the executor for potentially-blocking material loading
     * @param openTelemetry    the telemetry root, or {@code null} for {@link OpenTelemetry#noop()}
     * @return a {@link TlsFactoryInitContext}
     */
    public static TlsFactoryInitContext initContext(Map<String, String> params,
                                                    ScheduledExecutorService scheduler,
                                                    Executor blockingExecutor,
                                                    OpenTelemetry openTelemetry) {
        Map<String, String> safeParams = params == null ? Map.of() : Map.copyOf(params);
        OpenTelemetry ot = openTelemetry == null ? OpenTelemetry.noop() : openTelemetry;
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return safeParams;
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return blockingExecutor;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return ot;
            }
        };
    }

    /**
     * Initialize a factory and block until it is ready, per the fail-fast contract (a failed
     * {@code initialize} is fatal to the owning component's startup). Unwraps
     * {@link CompletionException}/{@link ExecutionException} to the underlying cause.
     *
     * @param factory the factory to initialize
     * @param context the init context
     * @throws Exception the underlying initialization failure, if any
     */
    public static void initializeBlocking(PulsarTlsFactory factory, TlsFactoryInitContext context)
            throws Exception {
        try {
            factory.initialize(context).get();
        } catch (ExecutionException e) {
            throw asException(e.getCause());
        } catch (CompletionException e) {
            throw asException(e.getCause());
        }
    }

    /**
     * Parse a {@code tlsFactoryConfig} string into the factory params map. A blank value yields an empty
     * map; a value starting with <code>{</code> is parsed as a JSON object; otherwise it is parsed as a
     * comma-separated {@code key=value} list.
     *
     * @param tlsFactoryConfig the configured factory params (may be null/blank)
     * @return an immutable params map (possibly empty)
     */
    public static Map<String, String> parseFactoryConfig(String tlsFactoryConfig) {
        if (StringUtils.isBlank(tlsFactoryConfig)) {
            return Map.of();
        }
        String trimmed = tlsFactoryConfig.trim();
        if (trimmed.startsWith("{")) {
            try {
                Map<String, String> parsed = ObjectMapperFactory.getMapper().reader()
                        .forType(new TypeReference<Map<String, String>>() {})
                        .readValue(trimmed);
                return parsed == null ? Map.of() : Map.copyOf(parsed);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse tlsFactoryConfig as a JSON object", e);
            }
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (String pair : trimmed.split(",")) {
            String entry = pair.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int eq = entry.indexOf('=');
            if (eq < 0) {
                map.put(entry, "");
            } else {
                map.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
            }
        }
        return Map.copyOf(map);
    }

    /**
     * Map a component's provider string to the Netty {@link SslProvider} engine used by the default
     * file-based factory. An explicit Netty engine literal is honored verbatim; JSSE provider names (e.g.
     * {@code Conscrypt}, {@code SunJSSE}) select no engine and map to {@link SslProvider#JDK} — they belong to
     * the other provider axis, and are routed there by {@link #resolveJsseProvider} (and passed to Jetty as
     * its JSSE provider on the web path).
     *
     * <p><b>Unset (the default) selects {@link SslProvider#OPENSSL_REFCNT} when the native engine is
     * available</b>, else {@link SslProvider#JDK}. Two reasons for that choice:
     *
     * <ul>
     *   <li><b>It restores the historical default.</b> The PIP-337 path passed a {@code null} provider to
     *       Netty, and {@code SslContextBuilder} then picks its own default — the native engine wherever
     *       {@code netty-tcnative} has a binary. Defaulting to {@code JDK} here would have silently moved
     *       every deployment onto the JDK engine.</li>
     *   <li><b>It keeps Pulsar off finalization.</b> {@code OPENSSL} and {@code OPENSSL_REFCNT} build the same
     *       native engine and both expose real reference counting — {@code OpenSslContext} does not stub out
     *       {@code retain()}/{@code release()}, it merely adds a {@code finalize()} that frees the native
     *       context if its owner forgot to. This factory never forgets: it owns each context it builds and
     *       releases it deterministically (see {@code FileBasedTlsFactory}), so the finalizer would only ever
     *       mask a bug. Finalization is deprecated for removal (JEP 421) and can already be switched off with
     *       {@code --finalization=disabled}, under which {@code OPENSSL} leaks exactly like an unreleased
     *       {@code OPENSSL_REFCNT} context.</li>
     * </ul>
     *
     * <p>An operator who explicitly configures {@code OPENSSL} still gets {@link SslProvider#OPENSSL},
     * unrewritten: silently substituting a different enum value than the one configured is the kind of
     * surprise this mapping exists to avoid. Live connections are unaffected by either variant — a Netty
     * engine retains its parent context for its lifetime and {@code SslHandler} releases the engine when it is
     * removed.
     *
     * @param providerString the component's provider string (may be null/blank)
     * @return the Netty {@link SslProvider} engine selection
     */
    public static SslProvider engineProvider(String providerString) {
        if (StringUtils.isNotBlank(providerString)) {
            String provider = providerString.trim();
            if ("OPENSSL".equalsIgnoreCase(provider)) {
                return SslProvider.OPENSSL;
            }
            if ("OPENSSL_REFCNT".equalsIgnoreCase(provider)) {
                return SslProvider.OPENSSL_REFCNT;
            }
            return SslProvider.JDK;
        }
        return OpenSsl.isAvailable() ? SslProvider.OPENSSL_REFCNT : SslProvider.JDK;
    }

    /**
     * Resolve the JSSE (SSLContext) provider for a server or broker-client TLS policy across the two provider
     * axes, mirroring {@code ClientTlsFactorySupport.resolveClientJsseProvider} for v4 parity. An explicit
     * {@code jsseProvider} wins; otherwise a {@code tlsProvider}/{@code sslProvider} value that is NOT a Netty
     * {@link SslProvider} engine literal (i.e. a JSSE provider name such as {@code Conscrypt} or {@code BCJSSE})
     * is routed to the JSSE axis, restoring the v4 keystore behavior where {@code tlsProvider=Conscrypt} built
     * the SSLContext with that provider. An engine literal ({@code JDK}/{@code OPENSSL}/{@code OPENSSL_REFCNT})
     * stays on the engine axis ({@link #engineProvider(String)}) and yields {@code null} here.
     *
     * @param explicitJsseProvider the configured {@code jsseProvider} (may be null/blank)
     * @param engineProviderString the configured {@code tlsProvider}/{@code sslProvider} (may be null/blank)
     * @return the resolved JSSE provider name, or {@code null} for none
     */
    /**
     * As {@link #resolveJsseProvider}, but for a <b>web listener</b>, whose JSSE provider defaults to
     * Conscrypt when nothing is configured and Conscrypt is usable on this platform.
     *
     * <p>Conscrypt was the shipped default for the web-listener provider keys before PIP-478 and is
     * meaningfully faster than the JDK provider, so it stays the default here. What changes is that the
     * default is now <em>conditional</em>: under PIP-337 the key only reached Jetty's
     * {@code SslContextFactory.setProvider(...)}, inert on a factory that overrides {@code getSslContext()},
     * so the shipped default never selected a provider and could not fail. Now that the value is pinned into
     * the built {@code SSLContext} it can, so it is applied only where Conscrypt actually loads.
     * {@code conscrypt-openjdk-uber} ships native libraries for x86_64 and — since 2.6.1 — aarch64, but not
     * for every platform Pulsar runs on (s390x, for one), and it need not be on the class path at all.
     *
     * <p>Hence availability rather than mere presence: {@link JcaProviders#CONSCRYPT_PROVIDER} is resolved by
     * calling Conscrypt's own {@code checkAvailability()}, which fails where the native library is missing, so
     * this returns {@code null} there and the JVM default applies. A provider the operator names explicitly is
     * always honoured verbatim and still fails loudly when it cannot be resolved — only the <em>default</em>
     * is conditional, because a default that breaks a supported platform is not a usable default.
     *
     * <p>This applies to server listeners only. Client-side hostname verification is unaffected: a server
     * does not verify hostnames, so pinning Conscrypt here cannot interact with the hostname verification
     * PIP-478 turns on.
     *
     * @param explicitJsseProvider the configured {@code jsseProvider} (may be null/blank)
     * @param engineProviderString the configured web-listener provider key (may be null/blank)
     * @return the resolved JSSE provider name, Conscrypt's name when nothing is configured and it is
     *         available, or {@code null} for the JVM default
     */
    public static String resolveWebJsseProvider(String explicitJsseProvider, String engineProviderString) {
        String configured = resolveJsseProvider(explicitJsseProvider, engineProviderString);
        if (configured != null) {
            return configured;
        }
        Provider conscrypt = JcaProviders.CONSCRYPT_PROVIDER;
        return conscrypt == null ? null : conscrypt.getName();
    }

    public static String resolveJsseProvider(String explicitJsseProvider, String engineProviderString) {
        if (StringUtils.isNotBlank(explicitJsseProvider)) {
            return explicitJsseProvider.trim();
        }
        if (StringUtils.isNotBlank(engineProviderString) && !isSslProviderEngineLiteral(engineProviderString)) {
            return engineProviderString.trim();
        }
        return null;
    }

    /**
     * Whether the provider string names a Netty {@link SslProvider} engine literal ({@code JDK} /
     * {@code OPENSSL} / {@code OPENSSL_REFCNT}, case-insensitive) rather than a JSSE (SSLContext) provider name.
     * Engine literals stay on the engine axis ({@link #engineProvider(String)} maps them to the Netty engine);
     * only other values are routed to the {@code jsseProvider} axis by {@link #resolveJsseProvider}.
     *
     * @param providerString the configured provider string (may be null/blank)
     * @return whether it is a Netty {@link SslProvider} engine literal
     */
    private static boolean isSslProviderEngineLiteral(String providerString) {
        if (StringUtils.isBlank(providerString)) {
            return false;
        }
        try {
            SslProvider.valueOf(providerString.trim().toUpperCase(Locale.ROOT));
            return true;
        } catch (IllegalArgumentException notAnEngineLiteral) {
            return false;
        }
    }

    private static Exception asException(Throwable cause) {
        if (cause == null) {
            return new Exception("TLS factory initialization failed");
        }
        if (cause instanceof Exception e) {
            return e;
        }
        return new Exception(cause);
    }
}
