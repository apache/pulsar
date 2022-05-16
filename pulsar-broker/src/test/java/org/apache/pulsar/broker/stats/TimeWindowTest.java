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

import org.junit.Test;
import org.testng.Assert;

public class TimeWindowTest {

    @Test
    public void windowTest() throws Exception {
        int intervalInMs = 1000;
        int sampleCount = 2;
        TimeWindow<Integer> timeWindow = new TimeWindow<>(sampleCount, intervalInMs);

        WindowWrap<Integer> expect1 = timeWindow.current(oldValue -> 1);
        WindowWrap<Integer> expect2 = timeWindow.current(oldValue -> null);
        Assert.assertNotNull(expect1);
        Assert.assertNotNull(expect2);

        if (expect1.start() == expect2.start()) {
            Assert.assertEquals((int) expect1.value(), 1);
            Assert.assertEquals(expect1, expect2);
            Assert.assertEquals(expect1.value(), expect2.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect3 = timeWindow.current(oldValue -> 2);
        WindowWrap<Integer> expect4 = timeWindow.current(oldValue -> null);
        Assert.assertNotNull(expect3);
        Assert.assertNotNull(expect4);

        if (expect3.start() == expect4.start()) {
            Assert.assertEquals((int) expect3.value(), 2);
            Assert.assertEquals(expect3, expect4);
            Assert.assertEquals(expect3.value(), expect4.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect5 = timeWindow.current(oldValue -> 3);
        WindowWrap<Integer> expect6 = timeWindow.current(oldValue -> null);
        Assert.assertNotNull(expect5);
        Assert.assertNotNull(expect6);

        if (expect5.start() == expect6.start()) {
            Assert.assertEquals((int) expect5.value(), 3);
            Assert.assertEquals(expect5, expect6);
            Assert.assertEquals(expect5.value(), expect6.value());
        }

        Thread.sleep(intervalInMs);

        WindowWrap<Integer> expect7 = timeWindow.current(oldValue -> 4);
        WindowWrap<Integer> expect8 = timeWindow.current(oldValue -> null);
        Assert.assertNotNull(expect7);
        Assert.assertNotNull(expect8);

        if (expect7.start() == expect8.start()) {
            Assert.assertEquals((int) expect7.value(), 4);
            Assert.assertEquals(expect7, expect8);
            Assert.assertEquals(expect7.value(), expect8.value());
        }
    }
}
