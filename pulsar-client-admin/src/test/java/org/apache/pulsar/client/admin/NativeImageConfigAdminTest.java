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
package org.apache.pulsar.client.admin;

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
import org.testng.annotations.Test;

/**
 * Validates the GraalVM native-image configuration files embedded under
 * {@code META-INF/native-image/org.apache.pulsar/pulsar-client-admin-original/}.
 *
 * <p>The admin client serializes a large set of model classes in
 * {@code org.apache.pulsar.common.*} over its REST API via Jackson, so each of those classes
 * must be registered for reflection. These tests catch stale or missing entries early — for
 * example, a model class renamed or removed in a future release would cause
 * {@link #reflectConfigClassesExist} to fail, and removing model coverage would cause
 * {@link #representativeModelClassesRegistered} to fail.
 *
 * <p>Note: this validates the Pulsar-owned metadata only. Library-level metadata for Jersey /
 * HK2 internals (if any is still required) is exercised end-to-end by the native-image smoke
 * test in the {@code pulsar-client-native-image} module.
 */
public class NativeImageConfigAdminTest {

    private static final String CONFIG_BASE =
            "META-INF/native-image/org.apache.pulsar/pulsar-client-admin-original/";

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
    public void representativeModelClassesRegistered() throws IOException {
        Set<String> registered = parseReflectConfigClassNames();

        // A representative sample of the policy/data model classes that travel over the
        // admin REST API as JSON. If any of these is dropped from the config, Jackson would
        // be unable to (de)serialize it in a native image. This is a guard against silently
        // losing model coverage — if you intentionally remove one, update this list too.
        String[] expectedModelClasses = {
                "org.apache.pulsar.common.policies.data.Policies",
                "org.apache.pulsar.common.policies.data.TenantInfo",
                "org.apache.pulsar.common.policies.data.TopicStats",
                "org.apache.pulsar.common.policies.data.PartitionedTopicStats",
                "org.apache.pulsar.common.policies.data.BacklogQuota",
                "org.apache.pulsar.common.policies.data.RetentionPolicies",
                "org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl",
                "org.apache.pulsar.common.policies.data.impl.BundlesDataImpl",
                "org.apache.pulsar.common.functions.FunctionConfig",
                "org.apache.pulsar.common.io.SinkConfig",
                "org.apache.pulsar.common.io.SourceConfig",
        };

        for (String modelClass : expectedModelClasses) {
            assertTrue(registered.contains(modelClass),
                    "Model class " + modelClass + " is not registered in reflect-config.json. "
                            + "Native-image builds will fail to (de)serialize it over the admin "
                            + "REST API.");
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
