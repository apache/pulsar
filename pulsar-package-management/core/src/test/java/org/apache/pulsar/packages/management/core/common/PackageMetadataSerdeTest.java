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
package org.apache.pulsar.packages.management.core.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException.MetadataFormatException;
import org.testng.annotations.Test;

public class PackageMetadataSerdeTest {

    private static PackageMetadata sampleMetadata() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put("testKey", "testValue");
        return PackageMetadata.builder()
                .description("test package metadata serialize and deserialize flow")
                .createTime(1_000L)
                .contact("test@apache.org")
                .modificationTime(2_000L)
                .properties(properties).build();
    }

    @Test
    public void testJsonRoundTrip() throws MetadataFormatException {
        PackageMetadata metadata = sampleMetadata();
        byte[] bytes = PackageMetadataUtil.toBytes(metadata, true);
        assertEquals(bytes[0], (byte) '{', "JSON-encoded metadata must start with '{'");
        PackageMetadata roundTripped = PackageMetadataUtil.fromBytes(bytes, true);
        assertEquals(roundTripped, metadata);
    }

    @Test
    public void testLegacyRoundTrip() throws MetadataFormatException {
        PackageMetadata metadata = sampleMetadata();
        byte[] bytes = PackageMetadataUtil.toBytes(metadata, false);
        assertEquals(bytes[0], (byte) 0xAC, "Java-serialized stream must start with STREAM_MAGIC byte 0xAC");
        assertEquals(bytes[1], (byte) 0xED, "Java-serialized stream must start with STREAM_MAGIC byte 0xED");
        PackageMetadata roundTripped = PackageMetadataUtil.fromBytes(bytes, true);
        assertEquals(roundTripped, metadata);
    }

    @Test
    public void testJsonReadableWhenLegacyDisabled() throws MetadataFormatException {
        PackageMetadata metadata = sampleMetadata();
        byte[] jsonBytes = PackageMetadataUtil.toBytes(metadata, true);
        // JSON is safe regardless of the legacy flag.
        PackageMetadata roundTripped = PackageMetadataUtil.fromBytes(jsonBytes, false);
        assertEquals(roundTripped, metadata);
    }

    @Test
    public void testLegacyRejectedWhenLegacyDisabled() {
        byte[] legacyBytes = PackageMetadataUtil.toBytes(sampleMetadata(), false);
        try {
            PackageMetadataUtil.fromBytes(legacyBytes, false);
            throw new AssertionError("Expected MetadataFormatException");
        } catch (MetadataFormatException ex) {
            assertTrue(ex.getMessage().contains("packagesManagementAllowLegacyJavaSerialization"),
                    "Rejection message should name the config flag to flip: " + ex.getMessage());
        }
    }

    @Test
    public void testFilterRejectsUnsafeClass() throws Exception {
        // Build a valid Java serialization stream whose top-level object is NOT a PackageMetadata
        // and whose class is outside the filter's allowlist. The JEP 290 filter must reject the
        // class descriptor BEFORE ObjectInputStream instantiates it.
        ArrayList<String> disallowed = new ArrayList<>();
        disallowed.add("gadget-chain-entry");
        byte[] bytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(disallowed);
            oos.flush();
            bytes = baos.toByteArray();
        }
        // Starts with the Java magic — fromBytes will dispatch to the legacy reader.
        assertEquals(bytes[0], (byte) 0xAC);
        assertEquals(bytes[1], (byte) 0xED);
        assertThrows(MetadataFormatException.class,
                () -> PackageMetadataUtil.fromBytes(bytes, true));
    }

    @Test
    public void testUnknownFormatRejected() {
        assertThrows(MetadataFormatException.class,
                () -> PackageMetadataUtil.fromBytes("plain text".getBytes(), true));
    }

    @Test
    public void testEmptyRejected() {
        assertThrows(MetadataFormatException.class,
                () -> PackageMetadataUtil.fromBytes(new byte[0], true));
        assertThrows(MetadataFormatException.class,
                () -> PackageMetadataUtil.fromBytes(null, true));
    }
}
