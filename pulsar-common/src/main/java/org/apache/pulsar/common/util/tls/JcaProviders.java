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
package org.apache.pulsar.common.util.tls;

import com.google.common.annotations.VisibleForTesting;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import javax.net.ssl.SSLContext;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

/**
 * Resolves the JCA/JCE security providers Pulsar relies on: a named provider for the PIP-478
 * {@code jsseProvider} / {@code jcaProvider} axes, the Bouncy Castle provider (FIPS or non-FIPS), and the
 * optional Conscrypt (OpenSSL) provider. This is the single provider-resolution primitive extracted from the
 * {@code SecurityUtility} grab-bag (PIP-478).
 *
 * <p><b>Bouncy Castle is resolved lazily and is optional.</b> Loading this class must not require it: the
 * class exists mainly to resolve <em>named</em> providers for the two TLS provider axes, which every TLS
 * policy does, whereas Bouncy Castle is needed only by the code that actually asks for it (message
 * encryption, and a deployment that pins {@code jcaProvider=BC}/{@code BCFIPS}). The provider is therefore
 * looked up on first use through {@link #bouncyCastleProvider()} — which reports absence as
 * {@link Optional#empty()} rather than failing — and only a caller that genuinely needs it, via
 * {@link #requireBouncyCastleProvider()}, turns absence into an error. Resolving it does install it
 * process-wide via {@code Security.addProvider}, as before.
 *
 * <p>Conscrypt, by contrast, is resolved during class initialization ({@link #CONSCRYPT_PROVIDER}) and
 * installed process-wide when present; it reports absence as {@code null} and never fails class loading.
 */
@CustomLog
public final class JcaProviders {

    public static final String BC_FIPS_PROVIDER_CLASS = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
    public static final String BC_NON_FIPS_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider";
    public static final String BC_JSSE_PROVIDER_CLASS = "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider";
    public static final String CONSCRYPT_PROVIDER_CLASS = "org.conscrypt.OpenSSLProvider";
    public static final Provider CONSCRYPT_PROVIDER = loadConscryptProvider();

    // Security.getProvider("BC") / Security.getProvider("BCFIPS").
    // also used to get Factories. e.g. CertificateFactory.getInstance("X.509", "BCFIPS")
    public static final String BC_FIPS = "BCFIPS";
    public static final String BC = "BC";

    /**
     * The name BouncyCastle's JSSE provider registers under — the same in FIPS and non-FIPS mode, because
     * the mode is a constructor argument rather than part of the provider identity.
     */
    public static final String BC_JSSE = "BCJSSE";

    private JcaProviders() {
    }

    /**
     * Force this class to initialize, which installs Conscrypt process-wide when it is on the classpath
     * (see {@link #CONSCRYPT_PROVIDER}). Consumers that need Conscrypt registered before they build a TLS
     * stack — but that do not otherwise touch this class — call this instead of referencing a field for
     * its side effect.
     *
     * @return whether Conscrypt was available and installed
     */
    public static boolean ensureConscryptRegistered() {
        return CONSCRYPT_PROVIDER != null;
    }

    /**
     * A resolved Bouncy Castle provider together with which of the two artifacts it came from. The flavour
     * travels with the provider because callers that care (FIPS-approved-only algorithm choices) would
     * otherwise have to re-derive it from the provider's class name, and because the answer is fixed once
     * the provider is resolved.
     *
     * @param provider the resolved, process-wide registered provider
     * @param fips     whether it is the FIPS-certified artifact ({@code bc-fips}, registered as
     *                 {@code BCFIPS}) rather than the general-purpose one ({@code bcprov}, registered as
     *                 {@code BC})
     */
    public record ResolvedBouncyCastleProvider(Provider provider, boolean fips) {
        public ResolvedBouncyCastleProvider {
            Objects.requireNonNull(provider, "provider must not be null");
        }
    }

    /**
     * Lazily resolves Bouncy Castle on first access, so that merely loading {@link JcaProviders} — which
     * every named-provider resolution does — never requires Bouncy Castle to be on the classpath.
     */
    private static final class BouncyCastleHolder {
        private static final ResolvedBouncyCastleProvider RESOLVED = loadBouncyCastleProvider();
    }

    /**
     * The Bouncy Castle provider, installed process-wide on first resolution:
     * <ol>
     *   <li>an already-registered {@code BC} or {@code BCFIPS} provider, if any;</li>
     *   <li>otherwise loaded from the classpath (non-FIPS preferred, FIPS as fallback) and registered via
     *       {@code Security.addProvider}.</li>
     * </ol>
     *
     * @return the resolved provider and its flavour, or {@link Optional#empty()} when Bouncy Castle is not
     *         available. Callers that cannot proceed without it should use
     *         {@link #requireBouncyCastleProvider()}, so the failure names them rather than surfacing as a
     *         class-initialization error somewhere unrelated.
     */
    public static Optional<ResolvedBouncyCastleProvider> bouncyCastleProvider() {
        return Optional.ofNullable(BouncyCastleHolder.RESOLVED);
    }

    /**
     * As {@link #bouncyCastleProvider()}, for callers whose functionality genuinely requires Bouncy Castle.
     *
     * @return the resolved provider and its flavour
     * @throws IllegalStateException when Bouncy Castle is not on the classpath
     */
    public static ResolvedBouncyCastleProvider requireBouncyCastleProvider() {
        return bouncyCastleProvider().orElseThrow(() -> new IllegalStateException(
                "No Bouncy Castle provider is available: neither an already-registered " + BC + "/" + BC_FIPS
                        + " provider nor " + BC_NON_FIPS_PROVIDER_CLASS + " / " + BC_FIPS_PROVIDER_CLASS
                        + " on the classpath. Add the bcprov (or bc-fips) dependency to use this feature."));
    }

    private static ResolvedBouncyCastleProvider loadBouncyCastleProvider() {
        Provider installed = Security.getProvider(BC);
        if (installed == null) {
            installed = Security.getProvider(BC_FIPS);
        }
        if (installed != null) {
            log.debug().attr("provider", installed.getName()).log("Already instantiated Bouncy Castle provider");
            return toResolvedProvider(installed);
        }
        // Not installed, try to load from the classpath. Absence is not an error here — it is only an error
        // for a caller that needs Bouncy Castle, which requireBouncyCastleProvider() reports.
        try {
            return toResolvedProvider(getBCProviderFromClassPath());
        } catch (Exception e) {
            log.debug().exception(e)
                    .log("No Bouncy Castle provider (FIPS or non-FIPS) on the classpath");
            return null;
        }
    }

    /**
     * Lazily resolves BouncyCastle's JSSE provider on first access, registering it from the classpath when
     * the operator has not installed it themselves.
     */
    private static final class BouncyCastleJsseHolder {
        private static final ResolvedBouncyCastleProvider RESOLVED = loadBouncyCastleJsseProvider();
    }

    /**
     * BouncyCastle's JSSE provider ({@value #BC_JSSE}), the provider a FIPS TLS deployment pins as
     * {@code jsseProvider}.
     *
     * <p>Unlike every other provider this class resolves, {@value #BC_JSSE} cannot be discovered: {@code bctls}
     * ships no {@code META-INF/services/java.security.Provider} entry, so an unregistered one is invisible to
     * {@link ServiceLoader}. And it cannot simply be default-constructed either, because its FIPS mode is a
     * constructor argument fixed at construction — the provider registers under the same name either way, so
     * a no-arg instance would quietly be the non-FIPS one. This resolves it explicitly instead:
     *
     * <ol>
     *   <li>an instance the operator already registered wins, whatever mode they built it in;</li>
     *   <li>otherwise, when {@code bctls} is on the classpath, it is constructed and registered here — in
     *       FIPS mode bound to the FIPS JCA provider when {@link #bouncyCastleProvider()} resolved that one,
     *       and in the default mode otherwise, so the TLS stack follows whichever BouncyCastle artifact the
     *       deployment actually ships;</li>
     *   <li>otherwise it is absent, and pinning {@code jsseProvider=BCJSSE} fails loudly.</li>
     * </ol>
     *
     * @return the resolved provider and whether it is in FIPS mode, or {@link Optional#empty()} when
     *         {@code bctls} is not on the classpath
     */
    public static Optional<ResolvedBouncyCastleProvider> bouncyCastleJsseProvider() {
        return Optional.ofNullable(BouncyCastleJsseHolder.RESOLVED);
    }

    private static ResolvedBouncyCastleProvider loadBouncyCastleJsseProvider() {
        Provider registered = Security.getProvider(BC_JSSE);
        if (registered != null) {
            log.debug().attr("provider", BC_JSSE).log("Already instantiated BouncyCastle JSSE provider");
            return new ResolvedBouncyCastleProvider(registered, isJsseFipsMode(registered));
        }
        Class<?> providerClass;
        try {
            providerClass = Class.forName(BC_JSSE_PROVIDER_CLASS);
        } catch (ClassNotFoundException e) {
            log.debug().attr("class", BC_JSSE_PROVIDER_CLASS)
                    .log("No BouncyCastle JSSE provider on the classpath");
            return null;
        }
        // Resolving the JCA provider first also installs it, which the FIPS configuration below needs: the
        // JSSE provider looks its crypto provider up by name while constructing.
        String config = jsseProviderConfig(bouncyCastleProvider().orElse(null));
        try {
            Provider provider = config == null
                    ? (Provider) providerClass.getDeclaredConstructor().newInstance()
                    : (Provider) providerClass.getDeclaredConstructor(String.class).newInstance(config);
            Security.addProvider(provider);
            boolean fips = isJsseFipsMode(provider);
            log.info().attr("provider", provider.getName()).attr("fips", fips)
                    .log("Registered the BouncyCastle JSSE provider");
            return new ResolvedBouncyCastleProvider(provider, fips);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn().attr("class", BC_JSSE_PROVIDER_CLASS).attr("config", config).exception(e)
                    .log("Failed to register the BouncyCastle JSSE provider");
            return null;
        }
    }

    /**
     * The constructor argument for a self-registered {@value #BC_JSSE}: {@code "fips:<name>"} binds it to a
     * FIPS JCA provider and turns FIPS mode on, while {@code null} selects the no-arg (default, non-FIPS)
     * constructor. FIPS mode is chosen only when the resolved BouncyCastle JCA provider is itself the FIPS
     * artifact — pairing a FIPS JSSE provider with a non-validated crypto provider would be FIPS-shaped
     * rather than FIPS-compliant, which is exactly what the two provider axes exist to prevent.
     *
     * @param jcaProvider the resolved BouncyCastle JCA provider, or {@code null} when there is none
     * @return the constructor argument, or {@code null} to use the no-arg constructor
     */
    @VisibleForTesting
    static String jsseProviderConfig(ResolvedBouncyCastleProvider jcaProvider) {
        if (jcaProvider == null || !jcaProvider.fips()) {
            return null;
        }
        return "fips:" + jcaProvider.provider().getName();
    }

    /** Query a JSSE provider's FIPS mode reflectively, so this class never needs {@code bctls} to compile. */
    private static boolean isJsseFipsMode(Provider provider) {
        try {
            return Boolean.TRUE.equals(provider.getClass().getMethod("isFipsMode").invoke(provider));
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.debug().attr("provider", provider.getName()).exception(e)
                    .log("Provider exposes no isFipsMode(); assuming non-FIPS");
            return false;
        }
    }

    /**
     * Classify a resolved provider. Both the registered name and the implementation class are checked: the
     * FIPS artifact registers as {@code BCFIPS}, and matching the class as well keeps the classification
     * right for a provider registered under a non-standard name.
     */
    private static ResolvedBouncyCastleProvider toResolvedProvider(Provider provider) {
        boolean fips = BC_FIPS.equals(provider.getName())
                || BC_FIPS_PROVIDER_CLASS.equals(provider.getClass().getCanonicalName());
        return new ResolvedBouncyCastleProvider(provider, fips);
    }

    private static Provider loadConscryptProvider() {
        Class<?> conscryptClazz;

        try {
            conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
            conscryptClazz.getMethod("checkAvailability").invoke(null);
        } catch (Throwable e) {
            if (e instanceof ClassNotFoundException) {
                log.debug("Conscrypt isn't available in the classpath. Using JDK default security provider.");
            } else if (e.getCause() instanceof UnsatisfiedLinkError) {
                log.debug().attr("os", System.getProperty("os.name")).attr("arch", System.getProperty("os.arch"))
                        .log("Conscrypt isn't available. Using JDK default security provider");
            } else {
                log.debug().attr("cause", e.getCause()).attr("reason", e.getMessage())
                        .log("Conscrypt isn't available. Using JDK default security provider");
            }
            return null;
        }

        Provider provider;
        try {
            provider = (Provider) Class.forName(CONSCRYPT_PROVIDER_CLASS).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            log.debug().attr("class", CONSCRYPT_PROVIDER_CLASS).exception(e)
                    .log("Unable to get security provider");
            return null;
        }

        // Unlike the PIP-337 loader this one installs NO custom hostname verifier, so Conscrypt keeps its
        // built-in default: SAN-only verification, with no fallback to the CN. That is stricter than the
        // fallback RFC 2818 section 3.1 mandates when a certificate carries no dNSName SAN, and stricter
        // than the last-resort CN-ID check RFC 6125 section 6.4.4 still permits. Since Conscrypt 2.6.0 a
        // TrustManagerImpl falls back to the default verifier on its own
        // (https://github.com/google/conscrypt/pull/1060 fixing issue 1015), so nothing has to be
        // propagated onto the individual trust managers.
        //
        // Nothing overrides that default any more: the CN-tolerant TlsHostnameVerifier that SecurityUtility
        // used to install process-wide was removed with it. So a Conscrypt-pinned client rejects a CN-only
        // server certificate that the JDK and OpenSSL engines still accept — the one deployment-visible
        // change from removing Pulsar's own CN-matching verifier.

        Security.addProvider(provider);
        log.debug().attr("provider", provider.getName()).attr("class", CONSCRYPT_PROVIDER_CLASS)
                .log("Added security provider");
        return provider;
    }

    /**
     * Get Bouncy Castle provider from classpath, and call Security.addProvider.
     * Throw Exception if failed.
     */
    private static Provider getBCProviderFromClassPath() throws Exception {
        Class<?> clazz;
        try {
            // prefer non FIPS, for backward compatibility concern.
            clazz = Class.forName(BC_NON_FIPS_PROVIDER_CLASS);
        } catch (ClassNotFoundException cnf) {
            log.warn().attr("nonFipsClass", BC_NON_FIPS_PROVIDER_CLASS).attr("fipsClass", BC_FIPS_PROVIDER_CLASS)
                    .log("Not able to get Bouncy Castle provider, try to get FIPS provider");
            // attempt to use the FIPS provider.
            clazz = Class.forName(BC_FIPS_PROVIDER_CLASS);
        }

        Provider provider = (Provider) clazz.getDeclaredConstructor().newInstance();
        Security.addProvider(provider);
        log.debug().attr("provider", provider.getName())
                .log("Found and Instantiated Bouncy Castle provider in classpath");
        return provider;
    }

    /**
     * Resolve a named {@link Provider} — e.g. the JSSE (SSLContext) provider named by the PIP-478
     * {@code jsseProvider} field. Resolution prefers a provider the operator has already registered in the
     * JVM ({@link Security#getProvider(String)}), falls back to the {@link ServiceLoader} mechanism on the
     * application (thread-context) class loader ({@code META-INF/services/java.security.Provider}) matching
     * by {@link Provider#getName()}, and fails loudly when the name resolves to nothing (the fail-fast
     * contract — a misconfigured provider surfaces at client build / server start rather than silently
     * defaulting).
     *
     * <p><b>Why the registered provider wins.</b> The two steps differ in <em>which instance</em> answers a
     * name: {@code ServiceLoader} constructs a fresh instance through the provider's <em>no-arg</em>
     * constructor, whereas {@link Security#getProvider(String)} returns the instance the operator installed.
     * Preferring the latter makes an explicit {@code java.security} registration authoritative, which is
     * what configuring one means — the operator's instance may carry provider-level configuration, and it is
     * the object actually present in the JVM's provider list. It matters concretely for any provider that
     * ships a {@code META-INF/services/java.security.Provider} entry <em>and</em> whose configuration is
     * constructor-supplied; {@code bcprov} ships such an entry, so before this ordering a pinned
     * {@code jcaProvider=BC} resolved to a fresh provider rather than the registered one.
     *
     * <p>Note that BouncyCastle's JSSE provider is <em>not</em> affected either way: {@code bctls} ships no
     * services entry, so {@code BCJSSE} is never {@code ServiceLoader}-discoverable and must be registered
     * by the operator to be resolvable at all. That is the only way its FIPS mode — a constructor argument,
     * {@code new BouncyCastleJsseProvider("fips:BCFIPS")}, with the provider registering under the plain
     * name {@code BCJSSE} either way — can reach Pulsar.
     *
     * <p>Two properties of the {@code ServiceLoader} step are worth stating. It <em>constructs</em> each
     * candidate provider as it iterates, because the name being matched lives on the instance
     * ({@link Provider#getName()}) rather than on the service descriptor — {@code ServiceLoader.stream()}
     * would defer construction but cannot answer a name without it, so the construction is unavoidable
     * here. And a provider resolved this way is returned <em>without</em> {@code Security.addProvider}:
     * resolution deliberately does not mutate global provider state, and every consumer hands the
     * resolved object straight to {@code getInstance(algorithm, provider)}. The consequence is that such
     * an instance is not the one a later {@link Security#getProvider(String)} would return.
     *
     * <p>Unlike {@link #resolveProvider(String)} (which falls back to the default provider), this never
     * returns {@code null} for a non-blank name: an unresolvable name is a configuration error.
     *
     * @param providerName the {@code java.security.Provider} name (blank/{@code null} returns {@code null})
     * @return the resolved provider, or {@code null} when {@code providerName} is blank
     * @throws IllegalArgumentException if a non-blank name resolves to no installed provider
     */
    public static Provider resolveNamedProvider(String providerName) {
        if (StringUtils.isBlank(providerName)) {
            return null;
        }
        String name = providerName.trim();
        // 1. A provider the operator registered in the JVM wins — it carries their configuration (see above).
        Provider registered = Security.getProvider(name);
        if (registered != null) {
            log.debug().attr("provider", name).log("Resolved JCA provider via Security.getProvider");
            return registered;
        }
        // 2. ServiceLoader on the application class loader (META-INF/services/java.security.Provider).
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = JcaProviders.class.getClassLoader();
        }
        // Iterate defensively: a single broken META-INF/services/java.security.Provider entry (an unrelated
        // provider whose class fails to load) throws ServiceConfigurationError from hasNext()/next(). Skipping
        // it — rather than aborting the whole loop — lets a good provider still resolve here, and otherwise
        // still falls through to the remaining steps rather than failing early.
        Iterator<Provider> it = ServiceLoader.load(Provider.class, classLoader).iterator();
        while (true) {
            Provider provider;
            try {
                if (!it.hasNext()) {
                    break;
                }
                provider = it.next();
            } catch (ServiceConfigurationError brokenEntry) {
                log.debug().exception(brokenEntry)
                        .log("Skipping a broken java.security.Provider ServiceLoader entry while resolving a "
                                + "named JCA provider");
                continue;
            }
            if (name.equals(provider.getName())) {
                log.debug().attr("provider", name).log("Resolved JCA provider via ServiceLoader");
                return provider;
            }
        }
        // 3. BouncyCastle's JSSE provider is invisible to both steps above until someone registers it —
        // bctls ships no services entry — so register it from the classpath on demand when it is the name
        // being pinned. See bouncyCastleJsseProvider() for why it cannot just be default-constructed.
        if (BC_JSSE.equals(name)) {
            Optional<ResolvedBouncyCastleProvider> jsse = bouncyCastleJsseProvider();
            if (jsse.isPresent()) {
                return jsse.get().provider();
            }
        }
        // 4. Fail loudly — a misconfigured provider must not silently default.
        throw new IllegalArgumentException("No java.security.Provider named '" + name + "' could be resolved via "
                + "Security.getProvider(...) or via ServiceLoader (META-INF/services/java.security.Provider) on the "
                + "application class loader. Ensure the provider is on the classpath and registered — a JSSE "
                + "(SSLContext) provider such as BCJSSE for jsseProvider (which additionally requires the bctls jar "
                + "on the classpath), or a JCA (KeyStore/CertificateFactory) "
                + "provider such as BCFIPS for jcaProvider.");
    }

    /**
     * Resolve a security {@link Provider} by name, falling back to the default {@code TLS}
     * {@code SSLContext} provider when the name is blank or unknown.
     */
    static Provider resolveProvider(String providerName) throws NoSuchAlgorithmException {
        Provider provider = null;
        if (!StringUtils.isEmpty(providerName)) {
            provider = Security.getProvider(providerName);
        }

        if (provider == null) {
            provider = SSLContext.getDefault().getProvider();
        }

        return provider;
    }

}
