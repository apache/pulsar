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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException;

public class PackageMetadataUtil {
    public static PackageMetadata fromBytes(byte[] bytes) throws PackagesManagementException.MetadataFormatException {
        try {
            Object o = SerializationUtils.deserialize(bytes);
            if (!(o instanceof PackageMetadata)) {
                throw new PackagesManagementException.MetadataFormatException("Unexpected metadata format");
            }
            return (PackageMetadata) o;
        } catch (Exception e) {
            throw new PackagesManagementException.MetadataFormatException("Unexpected error", e);
        }
    }

    public static byte[] toBytes(PackageMetadata packageMetadata) {
        return SerializationUtils.serialize(packageMetadata);
    }
}
