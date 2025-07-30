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
package org.apache.pulsar.zookeeper;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;

public class MaxValueMetadataNodePayloadLenEstimatorSplitPathTest {

    @Test
    public void testSplitPathWithNullInput() {
        // Test null input returns the meaningless split path result
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath(null);

        // Should return the static MEANINGLESS_SPLIT_PATH_RES instance
        assertEquals(result.partCount, 0);
        assertNull(result.parts[0]);
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithEmptyString() {
        // Test empty string returns the meaningless split path result
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("");

        assertEquals(result.partCount, 0);
        assertNull(result.parts[0]);
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithSingleCharacter() {
        // Test single character string returns the meaningless split path result
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/");

        assertEquals(result.partCount, 0);
        assertNull(result.parts[0]);
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithSingleSlash() {
        // Test single slash returns the meaningless split path result
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/");

        assertEquals(result.partCount, 0);
        assertNull(result.parts[0]);
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithTwoParts() {
        // Test path with two parts: /admin/clusters
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/clusters");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithThreeParts() {
        // Test path with three parts: /admin/policies/tenant
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/policies/tenant");

        assertEquals(result.partCount, 3);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "policies");
        // Third part should not be stored in parts array (only first two are kept)
    }

    @Test
    public void testSplitPathWithFourParts() {
        // Test path with four parts: /admin/policies/tenant/namespace
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/policies/tenant/namespace");

        assertEquals(result.partCount, 4);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "policies");
        // Only first two parts are stored in the parts array
    }

    @Test
    public void testSplitPathWithFiveParts() {
        // Test path with five parts: /admin/policies/tenant/namespace/topic
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/policies/tenant/namespace/topic");

        assertEquals(result.partCount, 5);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "policies");
    }

    @Test
    public void testSplitPathWithSixParts() {
        // Test path with six parts: /admin/partitioned-topics/persistent/tenant/namespace/topic
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/partitioned-topics/persistent/tenant/namespace/topic");

        assertEquals(result.partCount, 6);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "partitioned-topics");
    }

    @Test
    public void testSplitPathWithManagedLedgerPath() {
        // Test managed ledger path: /managed-ledgers/tenant/namespace/persistent/topic
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/managed-ledgers/tenant/namespace/persistent/topic");

        assertEquals(result.partCount, 5);
        assertEquals(result.parts[0], "managed-ledgers");
        assertEquals(result.parts[1], "tenant");
    }

    @Test
    public void testSplitPathWithSubscriptionPath() {
        // Test subscription path: /managed-ledgers/tenant/namespace/persistent/topic/subscription
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/managed-ledgers/tenant/namespace/persistent/topic/subscription");

        assertEquals(result.partCount, 6);
        assertEquals(result.parts[0], "managed-ledgers");
        assertEquals(result.parts[1], "tenant");
    }

    @Test
    public void testSplitPathWithTrailingSlash() {
        // Test path with trailing slash: /admin/clusters/
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/clusters/");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithMultipleTrailingSlashes() {
        // Test path with multiple trailing slashes: /admin/clusters///
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin/clusters///");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithConsecutiveSlashes() {
        // Test path with consecutive slashes: /admin//clusters
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin//clusters");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithLeadingSlashes() {
        // Test path with multiple leading slashes: ///admin/clusters
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("///admin/clusters");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithSinglePart() {
        // Test path with single part: /admin
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin");

        assertEquals(result.partCount, 1);
        assertEquals(result.parts[0], "admin");
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithEmptyParts() {
        // Test path with empty parts: /admin//clusters//tenant
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin//clusters//tenant");

        assertEquals(result.partCount, 3);
        assertEquals(result.parts[0], "admin");
        assertEquals(result.parts[1], "clusters");
    }

    @Test
    public void testSplitPathWithSpecialCharacters() {
        // Test path with special characters: /admin-test/clusters_test
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin-test/clusters_test");

        assertEquals(result.partCount, 2);
        assertEquals(result.parts[0], "admin-test");
        assertEquals(result.parts[1], "clusters_test");
    }

    @Test
    public void testSplitPathWithNumbers() {
        // Test path with numbers: /admin123/clusters456/tenant789
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/admin123/clusters456/tenant789");

        assertEquals(result.partCount, 3);
        assertEquals(result.parts[0], "admin123");
        assertEquals(result.parts[1], "clusters456");
    }

    @Test
    public void testSplitPathWithLongPath() {
        // Test very long path with many parts
        String longPath = "/part1/part2/part3/part4/part5/part6/part7/part8/part9/part10";
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath(longPath);

        assertEquals(result.partCount, 10);
        assertEquals(result.parts[0], "part1");
        assertEquals(result.parts[1], "part2");
        // Only first two parts are stored
    }

    @Test
    public void testSplitPathWithShortValidPath() {
        // Test shortest valid path: /a
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/a");

        assertEquals(result.partCount, 1);
        assertEquals(result.parts[0], "a");
        assertNull(result.parts[1]);
    }

    @Test
    public void testSplitPathWithTwoCharacterPath() {
        // Test two character path: /ab
        MaxValueMetadataNodePayloadLenEstimator.SplitPathRes result =
                MaxValueMetadataNodePayloadLenEstimator.splitPath("/ab");

        assertEquals(result.partCount, 1);
        assertEquals(result.parts[0], "ab");
        assertNull(result.parts[1]);
    }
}
