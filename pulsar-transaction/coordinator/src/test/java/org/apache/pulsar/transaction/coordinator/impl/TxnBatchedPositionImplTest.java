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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TxnBatchedPositionImplTest {

    @DataProvider(name = "batchSizeAndBatchIndexArgsArray")
    private Object[][] batchSizeAndBatchIndexArgsArray(){
        return new Object[][]{
                {10, 5},
                {64, 0},
                {64, 63},
                {230, 120},
                {256, 255}
        };
    }

    @Test(dataProvider = "batchSizeAndBatchIndexArgsArray")
    public void testSetAckSetByIndex(int batchSize, int batchIndex){
        // test 1/64
        TxnBatchedPositionImpl txnBatchedPosition = new TxnBatchedPositionImpl(1,1, batchSize, batchIndex);
        txnBatchedPosition.setAckSetByIndex();
        long[] ls = txnBatchedPosition.getAckSet();
        BitSetRecyclable bitSetRecyclable = BitSetRecyclable.valueOf(ls);
        for (int i = 0; i < batchSize; i++){
            if (i == batchIndex) {
                Assert.assertFalse(bitSetRecyclable.get(i));
            } else {
                Assert.assertTrue(bitSetRecyclable.get(i));
            }
        }
        bitSetRecyclable.recycle();
    }

    @DataProvider(name = "testHashcodeAndEqualsData")
    public Object[][] testHashcodeAndEqualsData(){
        Random random = new Random();
        return new Object[][]{
                {1,2, 10, 5},
                {123,1523, 64, 0},
                {random.nextInt(65535),random.nextInt(65535), 230, 120},
                {random.nextInt(65535),random.nextInt(65535), 256, 255}
        };
    }

    /**
     * Why is this test needed ？
     * {@link org.apache.bookkeeper.mledger.impl.ManagedCursorImpl} maintains batchIndexes, use {@link PositionImpl} or
     * {@link TxnBatchedPositionImpl} as the key. However, different maps may use "param-key.equals(key-in-map)" to
     * determine the contains, or use "key-in-map.equals(param-key)" or use "param-key.compareTo(key-in-map)" or use
     * "key-in-map.compareTo(param-key)" to determine the {@link Map#containsKey(Object)}, the these approaches may
     * return different results.
     * Note: In {@link java.util.concurrent.ConcurrentSkipListMap}, it use the {@link Comparable#compareTo(Object)} to
     *    determine whether the keys are the same. In {@link java.util.HashMap}, it use the
     *    {@link Object#hashCode()} & {@link  Object#equals(Object)} to determine whether the keys are the same.
     */
    @Test(dataProvider = "testHashcodeAndEqualsData")
    public void testKeyInMap(long ledgerId, long entryId, int batchSize, int batchIndex){
        // build data.
        Random random = new Random();
        int v = random.nextInt();
        PositionImpl position = new PositionImpl(ledgerId, entryId);
        TxnBatchedPositionImpl txnBatchedPosition = new TxnBatchedPositionImpl(position, batchSize, batchIndex);
        // ConcurrentSkipListMap.
        ConcurrentSkipListMap<PositionImpl, Integer> map1 = new ConcurrentSkipListMap<>();
        map1.put(position, v);
        Assert.assertTrue(map1.containsKey(txnBatchedPosition));
        ConcurrentSkipListMap<PositionImpl, Integer> map2 = new ConcurrentSkipListMap<>();
        map2.put(txnBatchedPosition, v);
        Assert.assertTrue(map2.containsKey(position));
        // HashMap.
        HashMap<PositionImpl, Integer> map3 = new HashMap<>();
        map3.put(position, v);
        Assert.assertTrue(map3.containsKey(txnBatchedPosition));
        HashMap<PositionImpl, Integer> map4 = new HashMap<>();
        map4.put(txnBatchedPosition, v);
        Assert.assertTrue(map4.containsKey(position));
        // ConcurrentHashMap.
        ConcurrentHashMap<PositionImpl, Integer> map5 = new ConcurrentHashMap<>();
        map5.put(position, v);
        Assert.assertTrue(map5.containsKey(txnBatchedPosition));
        ConcurrentHashMap<PositionImpl, Integer> map6 = new ConcurrentHashMap<>();
        map6.put(txnBatchedPosition, v);
        Assert.assertTrue(map6.containsKey(position));
        // LinkedHashMap.
        LinkedHashMap<PositionImpl, Integer> map7 = new LinkedHashMap<>();
        map7.put(position, v);
        Assert.assertTrue(map7.containsKey(txnBatchedPosition));
        LinkedHashMap<PositionImpl, Integer> map8 = new LinkedHashMap<>();
        map8.put(txnBatchedPosition, v);
        Assert.assertTrue(map8.containsKey(position));
    }

    /**
     * Why is this test needed ？
     * Make sure that when compare to the "markDeletePosition", it looks like {@link PositionImpl}
     * Note: In {@link java.util.concurrent.ConcurrentSkipListMap}, it use the {@link Comparable#compareTo(Object)} to
     *    determine whether the keys are the same. In {@link java.util.HashMap}, it use the
     *    {@link Object#hashCode()} & {@link  Object#equals(Object)} to determine whether the keys are the same.
     */
    @Test
    public void testCompareTo(){
        PositionImpl position = new PositionImpl(1, 1);
        TxnBatchedPositionImpl txnBatchedPosition = new TxnBatchedPositionImpl(position, 2, 0);
        Assert.assertEquals(position.compareTo(txnBatchedPosition), 0);
        Assert.assertEquals(txnBatchedPosition.compareTo(position), 0);
    }
}