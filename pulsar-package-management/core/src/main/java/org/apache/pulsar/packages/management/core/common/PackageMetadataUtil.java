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

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException.MetadataFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageMetadataUtil {

    private static final Logger log = LoggerFactory.getLogger(PackageMetadataUtil.class);

    private static final byte JSON_LEADING_BYTE = '{';
    private static final byte JAVA_MAGIC_BYTE_0 = (byte) 0xAC;
    private static final byte JAVA_MAGIC_BYTE_1 = (byte) 0xED;

    // Strict allowlist for the legacy Java-serialized read path. Only classes reachable from
    // PackageMetadata's declared field graph are permitted; everything else is rejected before
    // instantiation by the JEP 290 filter, blocking gadget chain entry points.
    private static final ObjectInputFilter LEGACY_FILTER = ObjectInputFilter.Config.createFilter(
            "maxdepth=5;maxrefs=100;maxbytes=1048576;maxarray=1024;"
                    + "org.apache.pulsar.packages.management.core.common.PackageMetadata;"
                    + "java.util.HashMap;java.util.LinkedHashMap;java.util.Map;java.util.Map$Entry;"
                    + "java.util.AbstractMap;"
                    + "java.lang.String;java.lang.Number;java.lang.Long;java.lang.Boolean;"
                    + "java.lang.Object;"
                    + "!*");

    private static final ObjectReader JSON_READER =
            ObjectMapperFactory.getMapper().reader().forType(PackageMetadata.class);
    private static final ObjectWriter JSON_WRITER = ObjectMapperFactory.getMapper().writer();

    private static final AtomicBoolean LEGACY_READ_WARNED = new AtomicBoolean(false);

    private PackageMetadataUtil() {
    }

    public static byte[] toBytes(PackageMetadata packageMetadata, boolean jsonSerializationEnabled) {
        if (jsonSerializationEnabled) {
            try {
                return JSON_WRITER.writeValueAsBytes(packageMetadata);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize package metadata as JSON", e);
            }
        }
        return SerializationUtils.serialize(packageMetadata);
    }

    public static PackageMetadata fromBytes(byte[] bytes, boolean allowLegacyJavaSerialization)
            throws MetadataFormatException {
        if (bytes == null || bytes.length == 0) {
            throw new MetadataFormatException("Empty package metadata");
        }
        int firstNonWhitespace = indexOfFirstNonWhitespace(bytes);
        if (firstNonWhitespace >= 0 && bytes[firstNonWhitespace] == JSON_LEADING_BYTE) {
            return readJson(bytes);
        }
        if (bytes.length >= 2 && bytes[0] == JAVA_MAGIC_BYTE_0 && bytes[1] == JAVA_MAGIC_BYTE_1) {
            if (!allowLegacyJavaSerialization) {
                throw new MetadataFormatException(
                        "Package metadata is in legacy Java serialization format but reading it is disabled. "
                                + "Enable packagesManagementAllowLegacyJavaSerialization or re-upload the package.");
            }
            return readLegacy(bytes);
        }
        throw new MetadataFormatException("Unrecognized package metadata format");
    }

    private static PackageMetadata readJson(byte[] bytes) throws MetadataFormatException {
        try {
            return JSON_READER.readValue(bytes);
        } catch (IOException e) {
            throw new MetadataFormatException("Failed to parse package metadata as JSON: " + e.getMessage());
        }
    }

    private static PackageMetadata readLegacy(byte[] bytes) throws MetadataFormatException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            ois.setObjectInputFilter(LEGACY_FILTER);
            Object o = ois.readObject();
            if (!(o instanceof PackageMetadata)) {
                throw new MetadataFormatException("Unexpected metadata type: "
                        + (o == null ? "null" : o.getClass().getName()));
            }
            if (LEGACY_READ_WARNED.compareAndSet(false, true)) {
                log.warn("Read a package metadata entry in the legacy Java serialization format. "
                        + "Re-upload packages or call updateMeta to migrate them to JSON, then disable "
                        + "packagesManagementAllowLegacyJavaSerialization.");
            }
            return (PackageMetadata) o;
        } catch (MetadataFormatException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataFormatException("Rejected legacy package metadata: " + e.getMessage());
        }
    }

    private static int indexOfFirstNonWhitespace(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            if (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
                return i;
            }
        }
        return -1;
    }

    // Source-compatible overloads for callers (including external integrations) that haven't
    // been updated to the explicit-flag form. Defaults track the production defaults.
    @Deprecated
    public static byte[] toBytes(PackageMetadata packageMetadata) {
        return toBytes(packageMetadata, true);
    }

    @Deprecated
    public static PackageMetadata fromBytes(byte[] bytes) throws MetadataFormatException {
        return fromBytes(bytes, true);
    }
}
