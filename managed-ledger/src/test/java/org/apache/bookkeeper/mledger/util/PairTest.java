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
package org.apache.bookkeeper.mledger.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PairTest {

    /*
     * Has been tested elsewhere
     *
     * @Test public void Pair() { }
     */

    @Test
    public void create() {
        Pair<String, String> p = Pair.create("firstOne", "secondOne");
        Assert.assertEquals("firstOne", p.first);
        Assert.assertEquals("secondOne", p.second);
        Integer int3 = new Integer(3);
        Pair<String, Integer> q = Pair.create("firstOne", int3);
        Assert.assertEquals("firstOne", q.first);
        Assert.assertEquals(int3, q.second);
    }

    @Test
    public void toStringTest() {
        Pair<String, String> p = Pair.create("firstOne", "secondOne");
        Assert.assertEquals("firstOne", p.first);
        Assert.assertEquals("secondOne", p.second);
        Assert.assertEquals("(firstOne,secondOne)", p.toString());
    }
}
