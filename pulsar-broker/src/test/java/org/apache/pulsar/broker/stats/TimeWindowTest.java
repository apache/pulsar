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
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.Test;

public class TimeWindowTest {

    @Test
    public void windowTest() throws Exception {
        int intervalInMs = 1000;
        int sampleCount = 2;
        TimeWindow<Integer> timeWindow = new TimeWindow<>(sampleCount, intervalInMs);

        WindowWrap<Integer> expect1 = timeWindow.current(oldValue -> 1);
        WindowWrap<Integer> expect2 = timeWindow.current(oldValue -> null);
        assertNotNull(expect1);
        assertNotNull(expect2);

        if (expect1.start() == expect2.start()) {
            assertEquals((int) expect1.value(), 1);
            assertEquals(expect1, expect2);
            assertEquals(expect1.value(), expect2.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect3 = timeWindow.current(oldValue -> 2);
        WindowWrap<Integer> expect4 = timeWindow.current(oldValue -> null);
        assertNotNull(expect3);
        assertNotNull(expect4);

        if (expect3.start() == expect4.start()) {
            assertEquals((int) expect3.value(), 2);
            assertEquals(expect3, expect4);
            assertEquals(expect3.value(), expect4.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect5 = timeWindow.current(oldValue -> 3);
        WindowWrap<Integer> expect6 = timeWindow.current(oldValue -> null);
        assertNotNull(expect5);
        assertNotNull(expect6);

        if (expect5.start() == expect6.start()) {
            assertEquals((int) expect5.value(), 3);
            assertEquals(expect5, expect6);
            assertEquals(expect5.value(), expect6.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect7 = timeWindow.current(oldValue -> 4);
        WindowWrap<Integer> expect8 = timeWindow.current(oldValue -> null);
        assertNotNull(expect7);
        assertNotNull(expect8);

        if (expect7.start() == expect8.start()) {
            assertEquals((int) expect7.value(), 4);
            assertEquals(expect7, expect8);
            assertEquals(expect7.value(), expect8.value());
        }
    }
}
