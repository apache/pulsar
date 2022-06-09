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
package org.apache.pulsar.common.naming;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetadataTests {

    @Test
    public void testValidMetadata() {
        Map<String, String> metadata = new HashMap<>();

        metadata.put(generateKey(1, 512), generateKey(1, 512));
        Assert.assertTrue(validateMetadata(metadata, 1024));

        metadata.clear();
        metadata.put(generateKey(1, 512), generateKey(1, 511));
        Assert.assertTrue(validateMetadata(metadata, 1024));

        metadata.clear();
        metadata.put(generateKey(1, 256), generateKey(1, 256));
        metadata.put(generateKey(2, 256), generateKey(2, 256));
        Assert.assertTrue(validateMetadata(metadata, 1024));

        metadata.clear();
        metadata.put(generateKey(1, 256), generateKey(1, 256));
        metadata.put(generateKey(2, 256), generateKey(2, 255));
        Assert.assertTrue(validateMetadata(metadata, 1024));
    }

    @Test
    public void testInvalidMetadata() {
        Map<String, String> metadata = new HashMap<>();

        metadata.put(generateKey(1, 512), generateKey(1, 513));
        Assert.assertFalse(validateMetadata(metadata, 1024));

        metadata.clear();
        metadata.put(generateKey(1, 256), generateKey(1, 256));
        metadata.put(generateKey(2, 256), generateKey(2, 257));
        Assert.assertFalse(validateMetadata(metadata, 1024));


        metadata.clear();
        metadata.put(generateKey(1, 256), generateKey(1, 256));
        metadata.put(generateKey(2, 256), generateKey(2, 256));
        metadata.put(generateKey(3, 1), generateKey(3, 1));
        Assert.assertFalse(validateMetadata(metadata, 1024));

        // set bigger maxConsumerMetadataSize, now validation should pass
        Assert.assertTrue(validateMetadata(metadata, 1024 * 10));
    }

    private static boolean validateMetadata(Map<String, String> metadata, int maxSize) {
        try {
            Metadata.validateMetadata(metadata, maxSize);
            return true;
        } catch (IllegalArgumentException ignore) {
            return false;
        }
    }

    private static String generateKey(int number, int length) {
        return IntStream.generate(() -> number).limit(length).mapToObj(Integer::toString)
                .collect(Collectors.joining(""));
    }
}
