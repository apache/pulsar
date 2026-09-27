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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.testng.annotations.Test;

/**
 * Validates the GraalVM native-image configuration files embedded under
 * {@code META-INF/native-image/org.apache.pulsar/pulsar-client-original/}.
 *
 * <p>These tests catch stale or missing entries early — for example, a class
 * renamed or removed in a future release would cause {@link #reflectConfigClassesExist}
 * to fail, and a new {@link Authentication} implementation added without updating
 * the config would cause {@link #allAuthenticationPluginsRegistered} to fail.
 */
public class NativeImageConfigTest {

    private static final String CONFIG_BASE =
            "META-INF/native-image/org.apache.pulsar/pulsar-client-original/";

    // ---- reflect-config.json ----

    @Test
    public void reflectConfigIsValidJson() throws IOException {
        try (InputStream is = openResource("reflect-config.json")) {
            JsonArray array = JsonParser.parseReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8)).getAsJsonArray();
            assertFalse(array.isEmpty(), "reflect-config.json must not be empty");
        }
    }

    @Test
    public void reflectConfigClassesExist() throws IOException {
        Set<String> classNames = parseReflectConfigClassNames();
        assertFalse(classNames.isEmpty());
        for (String className : classNames) {
            try {
                Class.forName(className, false, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                fail("reflect-config.json references a class that does not exist on the "
                        + "classpath: " + className + ". If this class was renamed or removed, "
                        + "update the native-image configuration.");
            }
        }
    }

    @Test
    public void allAuthenticationPluginsRegistered() throws IOException {
        Set<String> registered = parseReflectConfigClassNames();

        // Every Authentication implementation shipped with pulsar-client-original
        // must be registered for reflective constructor access so that
        // AuthenticationUtil.create() works in a native image.
        //
        // If you add a new Authentication implementation, add it here AND in
        // reflect-config.json.
        Class<?>[] expectedAuthPlugins = {
                AuthenticationDisabled.class,
                AuthenticationTls.class,
                AuthenticationToken.class,
                AuthenticationBasic.class,
                AuthenticationKeyStoreTls.class,
                AuthenticationOAuth2.class,
        };

        for (Class<?> authClass : expectedAuthPlugins) {
            assertTrue(registered.contains(authClass.getName()),
                    "Authentication implementation " + authClass.getName()
                            + " is not registered in reflect-config.json. Native-image builds "
                            + "will fail to instantiate this plugin via reflection.");
        }
    }

    // ---- native-image.properties ----

    @Test
    public void runtimeInitializedClassesExist() throws IOException {
        Set<String> classNames = parseRuntimeInitializedClassNames();
        assertFalse(classNames.isEmpty());
        for (String className : classNames) {
            try {
                Class.forName(className, false, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                fail("native-image.properties references a runtime-initialized class that "
                        + "does not exist on the classpath: " + className
                        + ". If this class was renamed or removed, update the "
                        + "native-image configuration.");
            }
        }
    }

    // ---- resource-config.json ----

    @Test
    public void resourceConfigIsValidJson() throws IOException {
        try (InputStream is = openResource("resource-config.json")) {
            JsonParser.parseReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8)).getAsJsonObject();
        }
    }

    // ---- helpers ----

    private InputStream openResource(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_BASE + fileName);
        assertNotNull(is, CONFIG_BASE + fileName + " not found on classpath");
        return is;
    }

    private Set<String> parseReflectConfigClassNames() throws IOException {
        Set<String> names = new HashSet<>();
        try (InputStream is = openResource("reflect-config.json")) {
            JsonArray array = JsonParser.parseReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8)).getAsJsonArray();
            for (JsonElement element : array) {
                String name = element.getAsJsonObject().get("name").getAsString();
                names.add(name);
            }
        }
        return names;
    }

    private Set<String> parseRuntimeInitializedClassNames() throws IOException {
        Set<String> names = new HashSet<>();
        try (InputStream is = openResource("native-image.properties")) {
            Properties props = new Properties();
            props.load(is);
            String args = props.getProperty("Args", "");
            // Extract class names from --initialize-at-run-time=com.foo.A,com.foo.B,...
            for (String arg : args.split("\\s+")) {
                if (arg.startsWith("--initialize-at-run-time=")) {
                    String value = arg.substring("--initialize-at-run-time=".length());
                    for (String cls : value.split(",")) {
                        String trimmed = cls.trim().replaceAll("[\\\\,]", "");
                        if (!trimmed.isEmpty()) {
                            names.add(trimmed);
                        }
                    }
                }
            }
        }
        return names;
    }
}
