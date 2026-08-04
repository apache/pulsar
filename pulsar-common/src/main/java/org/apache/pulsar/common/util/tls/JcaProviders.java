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

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

/**
 * Resolves the JCA/JCE security providers Pulsar relies on: the Bouncy Castle provider (FIPS or non-FIPS)
 * and the optional Conscrypt (OpenSSL) provider. This is the single provider-resolution primitive extracted
 * from the former {@code SecurityUtility} grab-bag (PIP-478); loading either provider (as a side effect of
 * class initialization, via {@link #BC_PROVIDER} / {@link #CONSCRYPT_PROVIDER}) calls
 * {@code Security.addProvider} so the provider is installed process-wide.
 */
@CustomLog
public final class JcaProviders {

    public static final Provider BC_PROVIDER = getProvider();
    public static final String BC_FIPS_PROVIDER_CLASS = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
    public static final String BC_NON_FIPS_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider";
    public static final String CONSCRYPT_PROVIDER_CLASS = "org.conscrypt.OpenSSLProvider";
    public static final Provider CONSCRYPT_PROVIDER = loadConscryptProvider();

    // Security.getProvider("BC") / Security.getProvider("BCFIPS").
    // also used to get Factories. e.g. CertificateFactory.getInstance("X.509", "BCFIPS")
    public static final String BC_FIPS = "BCFIPS";
    public static final String BC = "BC";

    private JcaProviders() {
    }

    public static boolean isBCFIPS() {
        return BC_PROVIDER.getClass().getCanonicalName().equals(BC_FIPS_PROVIDER_CLASS);
    }

    /**
     * Get the Bouncy Castle provider:
     *  1. return the already-registered BC or BCFIPS provider, if any;
     *  2. otherwise load it from the classpath (non-FIPS preferred, FIPS as fallback) and register it
     *     via Security.addProvider.
     */
    public static Provider getProvider() {
        boolean isProviderInstalled =
                Security.getProvider(BC) != null || Security.getProvider(BC_FIPS) != null;

        if (isProviderInstalled) {
            Provider provider = Security.getProvider(BC) != null
                    ? Security.getProvider(BC)
                    : Security.getProvider(BC_FIPS);
            log.debug().attr("provider", provider.getName()).log("Already instantiated Bouncy Castle provider");
            return provider;
        }

        // Not installed, try load from class path
        try {
            return getBCProviderFromClassPath();
        } catch (Exception e) {
            log.warn().exception(e)
                    .log("Not able to get Bouncy Castle provider for both FIPS and Non-FIPS from class path");
            throw new RuntimeException(e);
        }
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

        // Conscrypt is left with its built-in default hostname verifier, which performs standard RFC 2818
        // (SAN-based) hostname verification. Pulsar no longer installs a custom hostname verifier: the deprecated
        // CN-based hostname matching was removed in Pulsar 5.0 (PIP-478), so TLS certificates must carry the
        // hostname in the SubjectAltName (SAN) extension. The bug-1015 workaround in processConscryptTrustManager
        // still ensures Conscrypt's default (SAN) verifier is applied to each TrustManager.

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
     * {@code jsseProvider} field. Resolution uses
     * the {@link ServiceLoader} mechanism on the application (thread-context) class loader
     * ({@code META-INF/services/java.security.Provider}), matching by {@link Provider#getName()}; it falls
     * back to a provider already statically registered in the JVM ({@link Security#getProvider(String)}); and
     * it fails loudly when the name resolves to nothing (the fail-fast contract — a misconfigured provider
     * surfaces at client build / server start rather than silently defaulting).
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
        // 1. ServiceLoader on the application class loader (META-INF/services/java.security.Provider).
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = JcaProviders.class.getClassLoader();
        }
        // Iterate defensively: a single broken META-INF/services/java.security.Provider entry (an unrelated
        // provider whose class fails to load) throws ServiceConfigurationError from hasNext()/next(). Skipping
        // it — rather than aborting the whole loop — lets a good provider still resolve here, and otherwise
        // still falls through to the Security.getProvider fallback (step 2) rather than failing early.
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
        // 2. Fall back to a provider already statically registered in the JVM.
        Provider registered = Security.getProvider(name);
        if (registered != null) {
            log.debug().attr("provider", name).log("Resolved JCA provider via Security.getProvider");
            return registered;
        }
        // 3. Fail loudly — a misconfigured provider must not silently default.
        throw new IllegalArgumentException("No java.security.Provider named '" + name + "' could be resolved via "
                + "ServiceLoader (META-INF/services/java.security.Provider) on the application class loader or via "
                + "Security.getProvider(...). Ensure the provider is on the classpath and registered — a JSSE "
                + "(SSLContext) provider such as BCJSSE for jsseProvider, or a JCA (KeyStore/CertificateFactory) "
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

    /**
     * Ensures each Conscrypt {@link TrustManager} performs standard SAN-based (RFC 2818) hostname verification by
     * propagating Conscrypt's default {@code ConscryptHostnameVerifier} onto the trust manager instance. This is a
     * workaround for https://github.com/google/conscrypt/issues/1015, where a Conscrypt {@code TrustManagerImpl}
     * obtained through the standard JSSE {@link javax.net.ssl.TrustManagerFactory} does not automatically pick up
     * the default hostname verifier, when Conscrypt / OpenSSL is used as the TLS security provider.
     *
     * @param trustManagers the array of TrustManager instances to process.
     * @return same instance passed as parameter
     */
    static TrustManager[] processConscryptTrustManagers(TrustManager[] trustManagers) {
        for (TrustManager trustManager : trustManagers) {
            processConscryptTrustManager(trustManager);
        }
        return trustManagers;
    }

    // workaround https://github.com/google/conscrypt/issues/1015
    private static void processConscryptTrustManager(TrustManager trustManager) {
        if (trustManager.getClass().getName().equals("org.conscrypt.TrustManagerImpl")) {
            try {
                Class<?> conscryptClazz = Class.forName("org.conscrypt.Conscrypt");
                Object hostnameVerifier = conscryptClazz.getMethod("getHostnameVerifier",
                        new Class<?>[]{TrustManager.class}).invoke(null, trustManager);
                if (hostnameVerifier == null) {
                    Object defaultHostnameVerifier = conscryptClazz.getMethod("getDefaultHostnameVerifier",
                            new Class<?>[]{TrustManager.class}).invoke(null, trustManager);
                    if (defaultHostnameVerifier != null) {
                        conscryptClazz.getMethod("setHostnameVerifier", new Class<?>[]{
                                TrustManager.class,
                                Class.forName("org.conscrypt.ConscryptHostnameVerifier")
                        }).invoke(null, trustManager, defaultHostnameVerifier);
                    }
                }
            } catch (ReflectiveOperationException e) {
                log.warn().exception(e)
                        .log("Unable to set hostname verifier for Conscrypt TrustManager implementation");
            }
        }
    }
}
