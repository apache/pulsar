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
package org.apache.pulsar.broker.stats;

import static org.testng.Assert.assertEquals;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class DimensionStatsTest {
    @Test
    void shouldCalculateQuantiles() {
        DimensionStats dimensionStats = new DimensionStats("test", 100, false);
        for (int i = 1; i <= 10000; i++) {
            dimensionStats.recordDimensionTimeValue(i, TimeUnit.MILLISECONDS);
        }
        DimensionStats.DimensionStatsSnapshot snapshot = dimensionStats.getSnapshot();
        assertEquals(snapshot.getMeanDimension(), 5000, 1);
        assertEquals(snapshot.getMedianDimension(), 5000, 100);
        assertEquals(snapshot.getDimension75(), 7500, 100);
        assertEquals(snapshot.getDimension95(), 9500, 100);
        assertEquals(snapshot.getDimension99(), 9900, 100);
        assertEquals(snapshot.getDimension999(), 9990, 10);
        assertEquals(snapshot.getDimension9999(), 9999, 1);
        assertEquals(snapshot.getDimensionCount(), 10000);
        assertEquals(snapshot.getDimensionSum(), 50005000);
    }
}