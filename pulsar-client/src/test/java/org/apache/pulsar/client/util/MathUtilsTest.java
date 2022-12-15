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
package org.apache.pulsar.client.util;


import org.testng.Assert;
import org.testng.annotations.Test;

public class MathUtilsTest {

    @Test
    public void testCeilDiv() {
        Assert.assertEquals(MathUtils.ceilDiv(0, 1024), 0);
        Assert.assertEquals(MathUtils.ceilDiv(1, 1024), 1);
        Assert.assertEquals(MathUtils.ceilDiv(1023, 1024), 1);
        Assert.assertEquals(MathUtils.ceilDiv(1024, 1024), 1);
        Assert.assertEquals(MathUtils.ceilDiv(1025, 1024), 2);

        Assert.assertEquals(MathUtils.ceilDiv(0, Integer.MAX_VALUE), 0);
        Assert.assertEquals(MathUtils.ceilDiv(1, Integer.MAX_VALUE), 1);
        Assert.assertEquals(MathUtils.ceilDiv(Integer.MAX_VALUE - 1, Integer.MAX_VALUE), 1);
        Assert.assertEquals(MathUtils.ceilDiv(Integer.MAX_VALUE, Integer.MAX_VALUE), 1);
    }
}