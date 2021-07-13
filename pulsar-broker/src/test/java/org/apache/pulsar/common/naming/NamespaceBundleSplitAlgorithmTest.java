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


import static org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_NAME;
import static org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm.TOPIC_COUNT_EQUALLY_DIVIDE;
import org.testng.Assert;
import org.testng.annotations.Test;


public class NamespaceBundleSplitAlgorithmTest {

    @Test
    public void testOfMethodReturnCorrectValue() {
        NamespaceBundleSplitAlgorithm nullValue = NamespaceBundleSplitAlgorithm.of(null);
        Assert.assertNull(nullValue);
        NamespaceBundleSplitAlgorithm whatever = NamespaceBundleSplitAlgorithm.of("whatever");
        Assert.assertNull(whatever);
        NamespaceBundleSplitAlgorithm rangeEquallyDivideName = NamespaceBundleSplitAlgorithm.of(RANGE_EQUALLY_DIVIDE_NAME);
        Assert.assertTrue(rangeEquallyDivideName instanceof RangeEquallyDivideBundleSplitAlgorithm);
        NamespaceBundleSplitAlgorithm topicCountEquallyDivide = NamespaceBundleSplitAlgorithm.of(TOPIC_COUNT_EQUALLY_DIVIDE);
        Assert.assertTrue(topicCountEquallyDivide instanceof TopicCountEquallyDivideBundleSplitAlgorithm);
    }
}