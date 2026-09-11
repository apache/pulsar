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
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * PEM parsing edge cases for the shared {@link PemReader} primitive.
 *
 * <p>Covers the preamble scan in particular: the extracted-from implementation consumed two lines per
 * iteration while looking for {@code -----BEGIN}, so whether a key file parsed depended on the <em>parity</em>
 * of the number of lines preceding that marker. Real-world key files carry such preambles — an
 * {@code openssl} "Bag Attributes" block, a comment, or a stray blank line.
 */
public class PemReaderTest {

    private Path dir;

    @BeforeMethod
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("pip478-pem-");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (dir != null) {
            FileUtils.deleteDirectory(dir.toFile());
        }
    }

    @DataProvider(name = "privateKeyFormats")
    public static Object[][] privateKeyFormats() throws Exception {
        List<Object[]> cases = new ArrayList<>();
        for (String algorithm : List.of("RSA", "EC")) {
            KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
            generator.initialize("RSA".equals(algorithm) ? 2048 : 256);
            PrivateKey key = generator.generateKeyPair().getPrivate();
            Provider provider = KeyFactory.getInstance(algorithm).getProvider();
            for (boolean pkcs1 : new boolean[]{false, true}) {
                if (pkcs1 && !"RSA".equals(algorithm)) {
                    continue;
                }
                byte[] encoded = pkcs1
                        ? PrivateKeyInfo.getInstance(key.getEncoded()).parsePrivateKey().toASN1Primitive().getEncoded()
                        : key.getEncoded();
                String pem = toPem(pkcs1 ? "RSA PRIVATE KEY" : "PRIVATE KEY", encoded);
                for (int preambleLines = 0; preambleLines <= 4; preambleLines++) {
                    for (Provider pinned : new Provider[]{null, provider}) {
                        cases.add(new Object[]{pem, key, preambleLines, pinned});
                    }
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "privateKeyFormats")
    public void loadsPrivateKeyRegardlessOfFormatAndPreamble(String pem, PrivateKey expected, int preambleLines,
                                                            Provider provider) throws Exception {
        Path keyFile = writeKeyWithPreamble(preambleLines, pem);

        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(keyFile.toString(), provider);

        assertThat(key.getAlgorithm()).isEqualTo(expected.getAlgorithm());
        assertThat(key.getEncoded()).as("key with %s preamble line(s) parses", preambleLines)
                .isEqualTo(expected.getEncoded());
    }

    @DataProvider(name = "pemWhitespace")
    public static Object[][] pemWhitespace() throws Exception {
        List<Object[]> cases = new ArrayList<>();
        for (Object[] format : privateKeyFormats()) {
            if ((int) format[2] != 0 || format[3] != null) {
                continue;
            }
            for (String newline : List.of("\n", "\r\n", "\r")) {
                for (int whitespace = 0; whitespace < 4; whitespace++) {
                    StringBuilder pem = new StringBuilder();
                    for (String line : ((String) format[0]).lines().toList()) {
                        if (whitespace == 1) {
                            line = " \t" + line + "\t ";
                        } else if (whitespace == 2) {
                            pem.append(" \t").append(newline).append(newline);
                        } else if (whitespace == 3 && !line.startsWith("-----")) {
                            line = line.substring(0, 4) + " \t\f\u000b" + line.substring(4);
                        }
                        pem.append(line).append(newline);
                    }
                    cases.add(new Object[]{pem.toString(), format[1]});
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "pemWhitespace")
    public void loadsPrivateKeyWithWhitespace(String pem, PrivateKey expected) throws Exception {
        PrivateKey key = PemReader.loadPrivateKeyFromPemStream(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
        assertThat(key.getEncoded()).isEqualTo(expected.getEncoded());
    }

    @Test
    public void loadsPrivateKeyFromAnOpenSslBagAttributesPreamble() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        PrivateKey expected = generator.generateKeyPair().getPrivate();
        byte[] pkcs1 = PrivateKeyInfo.getInstance(expected.getEncoded())
                .parsePrivateKey().toASN1Primitive().getEncoded();
        List<String> lines = new ArrayList<>();
        lines.add("Bag Attributes");
        lines.add("    friendlyName: broker");
        lines.add("    localKeyID: 54 69 6D 65 20 31 32 33");
        lines.add("Key Attributes: <No Attributes>");
        Path keyFile = writeKeyWithPreamble(lines, toPem("RSA PRIVATE KEY", pkcs1));

        assertThat(PemReader.loadPrivateKeyFromPemFile(keyFile.toString()).getEncoded())
                .isEqualTo(expected.getEncoded());
    }

    @Test
    public void rejectsMalformedPkcs1Key() {
        String pem = toPem("RSA PRIVATE KEY", new byte[]{0x30, 0x00});
        assertThatThrownBy(() -> PemReader.loadPrivateKeyFromPemStream(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(KeyManagementException.class);
    }

    @Test
    public void doesNotFallBackFromPinnedProviderForPkcs1Key() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        byte[] pkcs1 = PrivateKeyInfo.getInstance(generator.generateKeyPair().getPrivate().getEncoded())
                .parsePrivateKey().toASN1Primitive().getEncoded();
        String pem = toPem("RSA PRIVATE KEY", pkcs1);
        Provider emptyProvider = new Provider("NoKeyFactories", "1.0", "No key factories for testing") { };
        assertThatThrownBy(() -> PemReader.loadPrivateKeyFromPemStream(
                new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)), emptyProvider))
                .isInstanceOf(KeyManagementException.class);
    }

    private static String toPem(String label, byte[] encoded) {
        return "-----BEGIN " + label + "-----\n"
                + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(encoded)
                + "\n-----END " + label + "-----\n";
    }

    private Path writeKeyWithPreamble(int preambleLines, String pem) throws Exception {
        List<String> preamble = new ArrayList<>();
        for (int i = 0; i < preambleLines; i++) {
            preamble.add("# preamble line " + i);
        }
        return writeKeyWithPreamble(preamble, pem);
    }

    private Path writeKeyWithPreamble(List<String> preamble, String pem) throws Exception {
        List<String> lines = new ArrayList<>(preamble);
        lines.addAll(pem.lines().toList());
        Path keyFile = dir.resolve("key-" + lines.size() + ".pem");
        Files.write(keyFile, lines, StandardCharsets.UTF_8);
        return keyFile;
    }
}
