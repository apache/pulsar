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
package org.apache.pulsar.client.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.testng.annotations.Test;

/**
 * Avro only gained {@code ClassSecurityValidator} in 1.12.2, and an application using
 * {@code pulsar-client-original} can pin an older Avro. Declaring trusted classes has to degrade to a
 * no-op there rather than failing — {@code AvroSchema.of} calls into this on every schema construction,
 * so a NoClassDefFoundError would surface as {@code Schema.AVRO(...)} itself breaking.
 *
 * <p>Loads {@code AvroTrustedClasses} in a class loader that hides the validator, which is the only way
 * to exercise the absent case while the test JVM itself has a current Avro.
 */
public class AvroTrustedClassesWithoutValidatorTest {

    private static final String VALIDATOR = "org.apache.avro.util.ClassSecurityValidator";

    @Test
    public void testDeclaringTrustIsANoOpWhenAvroHasNoValidator() throws Exception {
        try (URLClassLoader hidden = new HidingClassLoader(VALIDATOR)) {
            Class<?> trustedClasses = hidden.loadClass(AvroTrustedClasses.class.getName());
            // Loaded by the hiding loader, so this is genuinely the "no validator" world.
            assertThat(trustedClasses.getClassLoader()).isSameAs(hidden);

            Method trust = trustedClasses.getMethod("trust", Class[].class);
            Method trustPackages = trustedClasses.getMethod("trustPackages", String[].class);
            Method isInstalled = trustedClasses.getMethod("isInstalled");

            assertThatCode(() -> {
                trust.invoke(null, (Object) new Class<?>[] {String.class});
                trustPackages.invoke(null, (Object) new String[] {"com.example"});
            }).doesNotThrowAnyException();

            assertThat((Boolean) isInstalled.invoke(null)).isFalse();
        }
    }

    /** Delegates everything except the named classes, which it reports as absent. */
    private static final class HidingClassLoader extends URLClassLoader {

        private final String hiddenPrefix;

        HidingClassLoader(String hiddenPrefix) {
            // Load from the same class path as the test, but as a child of the platform loader so that
            // application classes resolve here rather than being delegated to the parent.
            super(classPathUrls(), ClassLoader.getPlatformClassLoader());
            this.hiddenPrefix = hiddenPrefix;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(hiddenPrefix)) {
                throw new ClassNotFoundException(name + " is hidden by this test");
            }
            return super.loadClass(name, resolve);
        }

        private static URL[] classPathUrls() {
            String[] entries = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
            URL[] urls = new URL[entries.length];
            for (int i = 0; i < entries.length; i++) {
                try {
                    urls[i] = new java.io.File(entries[i]).toURI().toURL();
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot build the test class path", e);
                }
            }
            return urls;
        }
    }
}
