/**
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

import java.util.HashMap;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PackageMetadataSerdeTest {
    @Test
    public void testPackageMetadataSerDe() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put("testKey", "testValue");
        PackageMetadata metadata = PackageMetadata.builder()
            .description("test package metadata serialize and deserialize flow")
            .createTime(System.currentTimeMillis())
            .contact("test@apache.org")
            .modificationTime(System.currentTimeMillis() + 1000)
            .properties(properties).build();

        byte[] metadataSerialized = PackageMetadataUtil.toBytes(metadata);

        try {
            PackageMetadata deSerializedMetadata = PackageMetadataUtil.fromBytes(metadataSerialized);
            Assert.assertEquals(metadata, deSerializedMetadata);
        } catch (PackagesManagementException.MetadataFormatException e) {
            Assert.fail("should not throw any exception");
        }

        try {
            byte[] failedMetadataSerialized = "wrong package metadata".getBytes();
            PackageMetadata deSerializedMetadata = PackageMetadataUtil.fromBytes(failedMetadataSerialized);
            Assert.fail("should throw the metadata format exception");
        } catch (PackagesManagementException.MetadataFormatException e) {
            // expected error
        }
    }
}
