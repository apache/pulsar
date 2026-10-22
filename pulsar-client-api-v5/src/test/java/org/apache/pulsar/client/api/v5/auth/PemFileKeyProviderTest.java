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
package org.apache.pulsar.client.api.v5.auth;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit coverage for {@link PemFileKeyProvider}: registering a key file → reading it
 * back as bytes; missing key → failed future; missing file on disk → failed future.
 */
public class PemFileKeyProviderTest {

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("pem-file-key-provider-test");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        if (tempDir != null) {
            try (var stream = Files.walk(tempDir)) {
                stream.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                                // best effort
                            }
                        });
            }
        }
    }

    @Test
    public void testReadRegisteredPublicKey() throws Exception {
        Path keyFile = Files.writeString(tempDir.resolve("pub.pem"), "PUBLIC-KEY-BYTES");
        var provider = PemFileKeyProvider.builder()
                .publicKey("orders-v1", keyFile)
                .build();

        EncryptionKey key = provider.getPublicKey("orders-v1").get();
        assertEquals(new String(key.key()), "PUBLIC-KEY-BYTES");
        assertTrue(key.metadata().isEmpty());
    }

    @Test
    public void testReadRegisteredPrivateKey() throws Exception {
        Path keyFile = Files.writeString(tempDir.resolve("priv.pem"), "PRIVATE-KEY-BYTES");
        var provider = PemFileKeyProvider.builder()
                .privateKey("orders-v1", keyFile)
                .build();

        EncryptionKey key = provider.getPrivateKey("orders-v1", Map.of()).get();
        assertEquals(new String(key.key()), "PRIVATE-KEY-BYTES");
    }

    @Test
    public void testUnknownKeyNameFailsFuture() {
        var provider = PemFileKeyProvider.builder().build();

        var ex = expectThrows(CompletionException.class,
                () -> provider.getPublicKey("missing").join());
        assertTrue(ex.getCause() instanceof IllegalArgumentException,
                "expected IllegalArgumentException, got: " + ex.getCause());
    }

    @Test
    public void testMissingFileOnDiskFailsFuture() {
        Path missing = tempDir.resolve("does-not-exist.pem");
        var provider = PemFileKeyProvider.builder()
                .publicKey("orders-v1", missing)
                .build();

        var ex = expectThrows(CompletionException.class,
                () -> provider.getPublicKey("orders-v1").join());
        assertTrue(ex.getCause() instanceof IOException,
                "expected IOException, got: " + ex.getCause());
    }

    @Test
    public void testProviderUsableAsBothSides() throws Exception {
        Path pub = Files.writeString(tempDir.resolve("pub.pem"), "PUB");
        Path priv = Files.writeString(tempDir.resolve("priv.pem"), "PRIV");
        var provider = PemFileKeyProvider.builder()
                .publicKey("k", pub)
                .privateKey("k", priv)
                .build();

        assertEquals(new String(provider.getPublicKey("k").get().key()), "PUB");
        assertEquals(new String(provider.getPrivateKey("k", Map.of()).get().key()), "PRIV");
    }
}
