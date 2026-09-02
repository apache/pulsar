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
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
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

    private static final String KEY = Resources.getResource(
            "certificate-authority/server-keys/broker.key-pk8.pem").getPath();

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

    @DataProvider(name = "preambleLineCounts")
    public static Object[][] preambleLineCounts() {
        // Both parities matter: pre-fix, even counts parsed and odd counts failed.
        return new Object[][]{{0}, {1}, {2}, {3}, {4}};
    }

    @Test(dataProvider = "preambleLineCounts")
    public void loadsPrivateKeyRegardlessOfPreambleLineCount(int preambleLines) throws Exception {
        Path keyFile = writeKeyWithPreamble(preambleLines);

        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(keyFile.toString());

        assertThat(key).as("key with %s preamble line(s) parses", preambleLines).isNotNull();
        assertThat(key.getAlgorithm()).isEqualTo("RSA");
    }

    @Test
    public void loadsPrivateKeyFromAnOpenSslBagAttributesPreamble() throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add("Bag Attributes");
        lines.add("    friendlyName: broker");
        lines.add("    localKeyID: 54 69 6D 65 20 31 32 33");
        lines.add("Key Attributes: <No Attributes>");
        Path keyFile = writeKeyWithPreamble(lines);

        assertThat(PemReader.loadPrivateKeyFromPemFile(keyFile.toString())).isNotNull();
    }

    private Path writeKeyWithPreamble(int preambleLines) throws Exception {
        List<String> preamble = new ArrayList<>();
        for (int i = 0; i < preambleLines; i++) {
            preamble.add("# preamble line " + i);
        }
        return writeKeyWithPreamble(preamble);
    }

    private Path writeKeyWithPreamble(List<String> preamble) throws Exception {
        List<String> lines = new ArrayList<>(preamble);
        lines.addAll(Files.readAllLines(Paths.get(KEY), StandardCharsets.UTF_8));
        Path keyFile = dir.resolve("key-" + lines.size() + ".pem");
        Files.write(keyFile, lines, StandardCharsets.UTF_8);
        return keyFile;
    }
}
