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
package org.apache.pulsar.common.naming;

import com.google.common.collect.Lists;
import java.util.Arrays;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class SpecifiedPositionsBundleSplitAlgorithmTest {

    @Test
    public void testTotalTopicsSizeLessThan1() {
        SpecifiedPositionsBundleSplitAlgorithm algorithm = new SpecifiedPositionsBundleSplitAlgorithm();
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        doReturn(CompletableFuture.completedFuture(Lists.newArrayList("a")))
                .when(mockNamespaceService).getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);
        assertNull(algorithm.getSplitBoundary(new BundleSplitOption(mockNamespaceService, mockNamespaceBundle,
                Arrays.asList(1L, 2L))).join());
    }

    @Test
    public void testSpecifiedPositionsLessThan1() {
        SpecifiedPositionsBundleSplitAlgorithm algorithm = new SpecifiedPositionsBundleSplitAlgorithm();
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        try {
            assertNull(algorithm.getSplitBoundary(
                    new BundleSplitOption(mockNamespaceService, mockNamespaceBundle, null)).join());
            fail("Should fail since split boundaries is null");
        } catch (IllegalArgumentException e) {
            // ignore
        }

        try {
            assertNull(algorithm.getSplitBoundary(
                    new BundleSplitOption(mockNamespaceService, mockNamespaceBundle, new ArrayList<>())).join());
            fail("Should fail since split boundaries is empty");
        } catch (IllegalArgumentException e) {
            // ignore
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    public void testAlgorithmReturnCorrectResult() {
        // -- algorithm
        SpecifiedPositionsBundleSplitAlgorithm algorithm = new SpecifiedPositionsBundleSplitAlgorithm();
        // -- calculate the mock result
        NamespaceService mockNamespaceService = mock(NamespaceService.class);
        NamespaceBundle mockNamespaceBundle = mock(NamespaceBundle.class);
        doReturn(1L).when(mockNamespaceBundle).getLowerEndpoint();
        doReturn(1000L).when(mockNamespaceBundle).getUpperEndpoint();
        doReturn(CompletableFuture.completedFuture(Lists.newArrayList("topic", "topic2")))
                .when(mockNamespaceService)
                .getOwnedTopicListForNamespaceBundle(mockNamespaceBundle);

        List<Long> positions = Arrays.asList(-1L, 0L, 1L, 100L, 200L, 500L, 800L, 1000L, 1100L);
        List<Long> splitPositions = algorithm.getSplitBoundary(
                new BundleSplitOption(mockNamespaceService, mockNamespaceBundle, positions)).join();

        assertEquals(splitPositions.size(), 4);
        assertTrue(splitPositions.contains(100L));
        assertTrue(splitPositions.contains(200L));
        assertTrue(splitPositions.contains(500L));
        assertTrue(splitPositions.contains(800L));
    }
}
