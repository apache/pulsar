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
package org.apache.pulsar.tests.integration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipFile;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.http.PulsarHttpClient;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.testng.annotations.Test;

public class ClientClasspathTest {
    @Test
    public void aggregateSupportsV4V5AndAdmin() throws Exception {
        boolean shaded = Boolean.getBoolean("testShadedClient");
        String prefix = shaded ? "org/apache/pulsar/shade/" : "";
        String otherPrefix = shaded ? "" : "org/apache/pulsar/shade/";
        ClassLoader loader = getClass().getClassLoader();
        for (String implementation : List.of("org/apache/pulsar/client/impl/ClientCnx.class",
                "org/apache/pulsar/client/impl/v5/PulsarClientProviderV5.class",
                "org/apache/pulsar/client/admin/internal/PulsarAdminImpl.class",
                "io/netty/channel/ChannelHandlerContext.class")) {
            assertEquals(Collections.list(loader.getResources(prefix + implementation)).size(), 1, implementation);
            assertNull(loader.getResource(otherPrefix + implementation), implementation);
        }
        assertEquals(Collections.list(loader.getResources(
                "META-INF/services/org.apache.pulsar.client.api.v5.internal.PulsarClientProvider")).size(), 1);
        try (PulsarClient v4 = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
             var v5 = org.apache.pulsar.client.api.v5.PulsarClient.builder()
                     .serviceUrl("pulsar://localhost:6650").build();
             PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build()) {
            assertNotNull(v4);
            assertNotNull(v5);
            assertNotNull(admin);
            assertEquals(Schema.string().decode(Schema.string().encode("v5")), "v5");
            URL v5Jar = v5.getClass().getProtectionDomain().getCodeSource().getLocation();
            URL adminJar = admin.getClass().getProtectionDomain().getCodeSource().getLocation();
            if (shaded) {
                assertEquals(adminJar, v5Jar);
                assertEquals(v4.getClass().getProtectionDomain().getCodeSource().getLocation(), v5Jar);
                // All five Pulsar API artifacts stay external and retain their original class names.
                try (ZipFile jar = new ZipFile(new File(v5Jar.toURI()))) {
                    for (Class<?> api : List.of(MessageId.class, PulsarAdmin.class, PulsarClientProvider.class,
                            PulsarTlsFactory.class, PulsarHttpClient.class)) {
                        String resource = api.getName().replace('.', '/') + ".class";
                        assertEquals(Collections.list(loader.getResources(resource)).size(), 1, resource);
                        assertNull(jar.getEntry(resource), resource);
                        assertNull(jar.getEntry(prefix + resource), resource);
                    }
                }
            } else {
                assertNotEquals(adminJar, v5Jar);
            }
        }
    }
}
