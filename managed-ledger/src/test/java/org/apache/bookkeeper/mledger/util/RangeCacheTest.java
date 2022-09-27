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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RangeCacheTest {

    class RefString extends AbstractReferenceCounted implements ReferenceCounted {
        String s;

        RefString(String s) {
            super();
            this.s = s;
            setRefCnt(1);
        }

        @Override
        protected void deallocate() {
            s = null;
        }

        @Override
        public ReferenceCounted touch(Object hint) {
            return this;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof RefString) {
                return this.s.equals(((RefString) obj).s);
            } else if (obj instanceof String) {
                return this.s.equals((String) obj);
            }

            return false;
        }
    }

    @Test
    public void simple() {
        RangeCache<Integer, RefString> cache = new RangeCache<>();

        cache.put(0, new RefString("0"));
        cache.put(1, new RefString("1"));

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);

        RefString s = cache.get(0);
        assertEquals(s.s, "0");
        assertEquals(s.refCnt(), 2);
        s.release();

        RefString s1 = cache.get(0);
        RefString s2 = cache.get(0);
        assertEquals(s1, s2);
        assertEquals(s1.refCnt(), 3);
        s1.release();
        s2.release();

        assertNull(cache.get(2));

        cache.put(2, new RefString("2"));
        cache.put(8, new RefString("8"));
        cache.put(11, new RefString("11"));

        assertEquals(cache.getSize(), 5);
        assertEquals(cache.getNumberOfEntries(), 5);

        cache.removeRange(1, 5, true);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(2, 8, false);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(0, 100, false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);

        cache.removeRange(0, 100, false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void customWeighter() {
        RangeCache<Integer, RefString> cache = new RangeCache<>(value -> value.s.length(), x -> 0);

        cache.put(0, new RefString("zero"));
        cache.put(1, new RefString("one"));

        assertEquals(cache.getSize(), 7);
        assertEquals(cache.getNumberOfEntries(), 2);
    }


    @Test
    public void customTimeExtraction() {
        RangeCache<Integer, RefString> cache = new RangeCache<>(value -> value.s.length(), x -> x.s.length());

        cache.put(1, new RefString("1"));
        cache.put(2, new RefString("22"));
        cache.put(3, new RefString("333"));
        cache.put(4, new RefString("4444"));

        assertEquals(cache.getSize(), 10);
        assertEquals(cache.getNumberOfEntries(), 4);

        Pair<Integer, Long> evictedSize = cache.evictLEntriesBeforeTimestamp(3);
        assertEquals(evictedSize.getRight().longValue(), 6);
        assertEquals(evictedSize.getLeft().longValue(), 3);

        assertEquals(cache.getSize(), 4);
        assertEquals(cache.getNumberOfEntries(), 1);
    }

    @Test
    public void doubleInsert() {
        RangeCache<Integer, RefString> cache = new RangeCache<>();

        RefString s0 = new RefString("zero");
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(0, s0));
        assertEquals(s0.refCnt(), 1);

        cache.put(1, new RefString("one"));

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        RefString s = cache.get(1);
        assertEquals(s.s, "one");
        assertEquals(s.refCnt(), 2);

        RefString s1 = new RefString("uno");
        assertEquals(s1.refCnt(), 1);
        assertFalse(cache.put(1, s1));
        assertEquals(s1.refCnt(), 1);
        s1.release();

        // Should not have been overridden in cache
        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.get(1).s, "one");
    }

    @Test
    public void getRange() {
        RangeCache<Integer, RefString> cache = new RangeCache<>();

        cache.put(0, new RefString("0"));
        cache.put(1, new RefString("1"));
        cache.put(3, new RefString("3"));
        cache.put(5, new RefString("5"));

        assertEquals(cache.getRange(1, 8),
                Lists.newArrayList(new RefString("1"), new RefString("3"), new RefString("5")));

        cache.put(8, new RefString("8"));
        assertEquals(cache.getRange(1, 8),
                Lists.newArrayList(new RefString("1"), new RefString("3"), new RefString("5"), new RefString("8")));

        cache.clear();
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void eviction() {
        RangeCache<Integer, RefString> cache = new RangeCache<>(value -> value.s.length(), x -> 0);

        cache.put(0, new RefString("zero"));
        cache.put(1, new RefString("one"));
        cache.put(2, new RefString("two"));
        cache.put(3, new RefString("three"));

        // This should remove the LRU entries: 0, 1 whose combined size is 7
        assertEquals(cache.evictLeastAccessedEntries(5), Pair.of(2, (long) 7));

        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.getSize(), 8);
        assertNull(cache.get(0));
        assertNull(cache.get(1));
        assertEquals(cache.get(2).s, "two");
        assertEquals(cache.get(3).s, "three");

        assertEquals(cache.evictLeastAccessedEntries(100), Pair.of(2, (long) 8));
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getSize(), 0);
        assertNull(cache.get(0));
        assertNull(cache.get(1));
        assertNull(cache.get(2));
        assertNull(cache.get(3));

        try {
            cache.evictLeastAccessedEntries(0);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            cache.evictLeastAccessedEntries(-1);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void evictions() {
        RangeCache<Integer, RefString> cache = new RangeCache<>();

        for (int i = 0; i < 100; i++) {
            cache.put(i, new RefString(Integer.toString(i)));
        }

        assertEquals(cache.getSize(), 100);
        Pair<Integer, Long> res = cache.evictLeastAccessedEntries(1);
        assertEquals((int) res.getLeft(), 1);
        assertEquals((long) res.getRight(), 1);
        assertEquals(cache.getSize(), 99);

        res = cache.evictLeastAccessedEntries(10);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 10);
        assertEquals(cache.getSize(), 89);

        res = cache.evictLeastAccessedEntries(100);
        assertEquals((int) res.getLeft(), 89);
        assertEquals((long) res.getRight(), 89);
        assertEquals(cache.getSize(), 0);

        for (int i = 0; i < 100; i++) {
            cache.put(i, new RefString(Integer.toString(i)));
        }

        assertEquals(cache.getSize(), 100);

        res = cache.removeRange(10, 20, false);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 10);
        assertEquals(cache.getSize(), 90);
    }

    @Test
    public void testInParallel() {
        RangeCache<String, RefString> cache = new RangeCache<>(value -> value.s.length(), x -> 0);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(cache::clear, 10, 10, TimeUnit.MILLISECONDS);
        for (int i = 0; i < 1000; i++) {
            cache.put(UUID.randomUUID().toString(), new RefString("zero"));
        }
        executor.shutdown();
    }

    @Test
    public void testPutSameObj() {
        RangeCache<Integer, RefString> cache = new RangeCache<>(value -> value.s.length(), x -> 0);
        RefString s0 = new RefString("zero");
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(0, s0));
        assertFalse(cache.put(0, s0));
    }
}
