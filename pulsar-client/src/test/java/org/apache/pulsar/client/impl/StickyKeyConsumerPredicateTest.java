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

package org.apache.pulsar.client.impl;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.junit.Assert;
import org.junit.Test;

public class StickyKeyConsumerPredicateTest {

    @Test
    public void testDecodePredicate4HashRangeAutoSplitStickyKeyConsumerSelector(){
        String predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson =
                "{\"lowHash\":1,\"highHash\":100,\"predicateType\":1,\"rangeSize\":65535}";
        StickyKeyConsumerPredicate keyConsumerPredicate = StickyKeyConsumerPredicate
                .decode(predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson);
        Assert.assertTrue(keyConsumerPredicate != null);
        Assert.assertTrue(keyConsumerPredicate
                        instanceof StickyKeyConsumerPredicate.Predicate4HashRangeAutoSplitStickyKeyConsumerSelector);
        StickyKeyConsumerPredicate.Predicate4HashRangeAutoSplitStickyKeyConsumerSelector predicate =
                (StickyKeyConsumerPredicate.Predicate4HashRangeAutoSplitStickyKeyConsumerSelector)
                        keyConsumerPredicate;
        Assert.assertEquals(1, predicate.getLowHash());
        Assert.assertEquals(100, predicate.getHighHash());
    }

    @Test
    public void testDecodePredicate4HashRangeExclusiveStickyKeyConsumerSelector(){
        String predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson =
                "{\"predicateType\":2,\"rangeMap\":{\"0\":\"__special_consumer_mark__\","
                        + "\"10\":\"__special_consumer_mark__\"},\"rangeSize\":65536}";
        StickyKeyConsumerPredicate keyConsumerPredicate = StickyKeyConsumerPredicate
                .decode(predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson);
        Assert.assertTrue(keyConsumerPredicate != null);
        Assert.assertTrue(keyConsumerPredicate
                        instanceof StickyKeyConsumerPredicate.Predicate4HashRangeExclusiveStickyKeyConsumerSelector);
        StickyKeyConsumerPredicate.Predicate4HashRangeExclusiveStickyKeyConsumerSelector predicate =
                (StickyKeyConsumerPredicate.Predicate4HashRangeExclusiveStickyKeyConsumerSelector)
                        keyConsumerPredicate;
        ConcurrentSkipListMap<Integer, String> map = new ConcurrentSkipListMap();
        map.put(0, StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        map.put(10, StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK);
        Assert.assertEquals(map, predicate.getRangeMap());
        Assert.assertEquals(65536, predicate.getRangeSize());
    }

    @Test
    public void testDecodePredicate4ConsistentHashingStickyKeyConsumerSelector(){
        String predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson =
                "{\"predicateType\":3,\"hashRing\":{\"0\":[\"__special_consumer_mark__\"],"
                        + "\"10\":[\"__special_consumer_mark__\"]},\"rangeSize\":65536}";
        StickyKeyConsumerPredicate keyConsumerPredicate = StickyKeyConsumerPredicate
                .decode(predicate4HashRangeAutoSplitStickyKeyConsumerSelectorJson);
        Assert.assertTrue(keyConsumerPredicate != null);
        Assert.assertTrue(keyConsumerPredicate
                        instanceof StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector);
        StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector predicate =
                (StickyKeyConsumerPredicate.Predicate4ConsistentHashingStickyKeyConsumerSelector)
                        keyConsumerPredicate;
        NavigableMap<Integer, List<String>> map = new ConcurrentSkipListMap();
        map.put(0, Arrays.asList(StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK));
        map.put(10, Arrays.asList(StickyKeyConsumerPredicate.SPECIAL_CONSUMER_MARK));
        Assert.assertEquals(map, predicate.getHashRing());
    }
}
