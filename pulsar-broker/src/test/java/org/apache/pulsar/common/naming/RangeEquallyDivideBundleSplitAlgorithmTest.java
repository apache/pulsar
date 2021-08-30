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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RangeEquallyDivideBundleSplitAlgorithmTest {

    @Test
    public void testGetSplitBoundaryMethodReturnCorrectResult() {
        RangeEquallyDivideBundleSplitAlgorithm rangeEquallyDivideBundleSplitAlgorithm = new RangeEquallyDivideBundleSplitAlgorithm();
        Assert.assertThrows(NullPointerException.class, () -> rangeEquallyDivideBundleSplitAlgorithm.getSplitBoundary(null, null));
        long lowerRange = 10L;
        long upperRange = 0xffffffffL;
        long correctResult = lowerRange + (upperRange - lowerRange) / 2;
        NamespaceBundle namespaceBundle = new NamespaceBundle(NamespaceName.SYSTEM_NAMESPACE, Range.range(lowerRange, BoundType.CLOSED, upperRange, BoundType.CLOSED),
                Mockito.mock(NamespaceBundleFactory.class));
        CompletableFuture<Long> splitBoundary = rangeEquallyDivideBundleSplitAlgorithm.getSplitBoundary(null, namespaceBundle);
        Long value = splitBoundary.join();
        Assert.assertEquals((long) value, correctResult);
    }
}