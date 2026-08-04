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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.security.Security;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

/**
 * Unit tests for the PIP-478 {@link JcaProviders#resolveNamedProvider(String)} JCA-provider resolution.
 */
public class JcaProvidersTest {

    @Test
    public void blankNameResolvesToNull() {
        assertThat(JcaProviders.resolveNamedProvider(null)).isNull();
        assertThat(JcaProviders.resolveNamedProvider("")).isNull();
        assertThat(JcaProviders.resolveNamedProvider("   ")).isNull();
    }

    @Test
    public void resolvesAnAlreadyRegisteredProvider() {
        // SunJSSE is a built-in provider registered in every JVM; it is found via the Security.getProvider
        // fallback (not ServiceLoader, which only surfaces META-INF/services-declared providers).
        Provider provider = JcaProviders.resolveNamedProvider("SunJSSE");
        assertThat(provider).isNotNull();
        assertThat(provider.getName()).isEqualTo("SunJSSE");
    }

    @Test
    public void unresolvableNameFailsLoud() {
        assertThatThrownBy(() -> JcaProviders.resolveNamedProvider("NoSuchCryptoProvider_pip478"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NoSuchCryptoProvider_pip478");
    }

    /** A minimal java.security.Provider registered at runtime (resolvable only via Security.getProvider). */
    private static final class DummyProvider extends Provider {
        private DummyProvider(String name) {
            super(name, "1.0", "PIP-478 test provider");
        }
    }

    // PIP-478 (FIX): a single broken META-INF/services/java.security.Provider entry (an unrelated provider whose
    // class cannot be loaded) throws ServiceConfigurationError mid-ServiceLoader-iteration. resolveNamedProvider
    // must SKIP it and still reach the Security.getProvider fallback, rather than aborting — otherwise a good,
    // registered provider becomes unresolvable just because some unrelated provider on the classpath is broken.
    @Test
    public void brokenServiceLoaderEntryDoesNotAbortSecurityProviderFallback() throws Exception {
        String providerName = "Pip478BrokenEntryTestProvider";
        Provider dummy = new DummyProvider(providerName);
        Security.addProvider(dummy); // resolvable via Security.getProvider, NOT via ServiceLoader
        Path tempDir = Files.createTempDirectory("pip478-broken-provider-");
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        URLClassLoader brokenLoader = null;
        try {
            // Plant a service file naming a non-existent provider class -> ServiceLoader throws
            // ServiceConfigurationError when it reaches this entry (no ServiceLoader provider matches our
            // runtime-only dummy name, so iteration is fully drained and necessarily hits the broken entry).
            Path servicesFile = tempDir.resolve("META-INF/services/java.security.Provider");
            Files.createDirectories(servicesFile.getParent());
            Files.writeString(servicesFile, "org.apache.pulsar.pip478.DoesNotExistProvider\n");
            brokenLoader = new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, previous);
            Thread.currentThread().setContextClassLoader(brokenLoader);

            Provider resolved = JcaProviders.resolveNamedProvider(providerName);
            assertThat(resolved).as("resolved via the Security.getProvider fallback despite the broken entry")
                    .isSameAs(dummy);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            Security.removeProvider(providerName);
            if (brokenLoader != null) {
                brokenLoader.close();
            }
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
