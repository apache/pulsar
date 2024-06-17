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
package org.apache.pulsar.common.util.collections;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

// From https://github.com/openjdk/jdk/blob/master/test/jdk/java/util/BitSet/BSMethods.java
@SuppressWarnings("all")
public class RoaringBitSetTest {
    private static Random generator = new Random();

    @Test
    public void testSetGetClearFlip() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet testSet = new RoaringBitSet();
            HashSet<Integer> history = new HashSet<>();

            // Set a random number of bits in random places
            // up to a random maximum
            int nextBitToSet = 0;
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;
            for (int x = 0; x < numberOfSetBits; x++) {
                nextBitToSet = generator.nextInt(highestPossibleSetBit);
                history.add(Integer.valueOf(nextBitToSet));
                testSet.set(nextBitToSet);
            }

            // Make sure each bit is set appropriately
            for (int x = 0; x < highestPossibleSetBit; x++) {
                if (testSet.get(x) != history.contains(Integer.valueOf(x)))
                    failCount++;
            }

            // Clear the bits
            Iterator<Integer> setBitIterator = history.iterator();
            while (setBitIterator.hasNext()) {
                Integer setBit = setBitIterator.next();
                testSet.clear(setBit.intValue());
            }

            // Verify they were cleared
            for (int x = 0; x < highestPossibleSetBit; x++)
                if (testSet.get(x))
                    failCount++;
            if (testSet.length() != 0)
                failCount++;

            // Set them with set(int, boolean)
            setBitIterator = history.iterator();
            while (setBitIterator.hasNext()) {
                Integer setBit = setBitIterator.next();
                testSet.set(setBit.intValue(), true);
            }

            // Make sure each bit is set appropriately
            for (int x = 0; x < highestPossibleSetBit; x++) {
                if (testSet.get(x) != history.contains(Integer.valueOf(x)))
                    failCount++;
            }

            // Clear them with set(int, boolean)
            setBitIterator = history.iterator();
            while (setBitIterator.hasNext()) {
                Integer setBit = setBitIterator.next();
                testSet.set(setBit.intValue(), false);
            }

            // Verify they were cleared
            for (int x = 0; x < highestPossibleSetBit; x++)
                if (testSet.get(x))
                    failCount++;
            if (testSet.length() != 0)
                failCount++;

            // Flip them on
            setBitIterator = history.iterator();
            while (setBitIterator.hasNext()) {
                Integer setBit = setBitIterator.next();
                testSet.flip(setBit.intValue());
            }

            // Verify they were flipped
            for (int x = 0; x < highestPossibleSetBit; x++) {
                if (testSet.get(x) != history.contains(Integer.valueOf(x)))
                    failCount++;
            }

            // Flip them off
            setBitIterator = history.iterator();
            while (setBitIterator.hasNext()) {
                Integer setBit = setBitIterator.next();
                testSet.flip(setBit.intValue());
            }

            // Verify they were flipped
            for (int x = 0; x < highestPossibleSetBit; x++)
                if (testSet.get(x))
                    failCount++;
            if (testSet.length() != 0)
                failCount++;

            checkSanity(testSet);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testClear() {
        int failCount = 0;

        for (int i = 0; i < 1000; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Make a fairly random bitset
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++)
                b1.set(generator.nextInt(highestPossibleSetBit));

            RoaringBitSet b2 = (RoaringBitSet) b1.clone();

            // Clear out a random range
            int rangeStart = generator.nextInt(100);
            int rangeEnd = rangeStart + generator.nextInt(100);

            // Use the clear(int, int) call on b1
            b1.clear(rangeStart, rangeEnd);

            // Use a loop on b2
            for (int x = rangeStart; x < rangeEnd; x++)
                b2.clear(x);

            // Verify their equality
            if (!b1.equals(b2)) {
                failCount++;
            }
            checkEquality(b1, b2);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testFlip() {
        int failCount = 0;

        for (int i = 0; i < 1000; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Make a fairly random bitset
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++)
                b1.set(generator.nextInt(highestPossibleSetBit));

            RoaringBitSet b2 = (RoaringBitSet) b1.clone();

            // Flip a random range
            int rangeStart = generator.nextInt(100);
            int rangeEnd = rangeStart + generator.nextInt(100);

            // Use the flip(int, int) call on b1
            b1.flip(rangeStart, rangeEnd);

            // Use a loop on b2
            for (int x = rangeStart; x < rangeEnd; x++)
                b2.flip(x);

            // Verify their equality
            if (!b1.equals(b2))
                failCount++;
            checkEquality(b1, b2);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testSet() {
        int failCount = 0;

        // Test set(int, int)
        for (int i = 0; i < 1000; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Make a fairly random bitset
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++)
                b1.set(generator.nextInt(highestPossibleSetBit));

            RoaringBitSet b2 = (RoaringBitSet) b1.clone();

            // Set a random range
            int rangeStart = generator.nextInt(100);
            int rangeEnd = rangeStart + generator.nextInt(100);

            // Use the set(int, int) call on b1
            b1.set(rangeStart, rangeEnd);

            // Use a loop on b2
            for (int x = rangeStart; x < rangeEnd; x++)
                b2.set(x);

            // Verify their equality
            if (!b1.equals(b2)) {
                failCount++;
            }
            checkEquality(b1, b2);
        }

        // Test set(int, int, boolean)
        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Make a fairly random bitset
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++)
                b1.set(generator.nextInt(highestPossibleSetBit));

            RoaringBitSet b2 = (RoaringBitSet) b1.clone();
            boolean setOrClear = generator.nextBoolean();

            // Set a random range
            int rangeStart = generator.nextInt(100);
            int rangeEnd = rangeStart + generator.nextInt(100);

            // Use the set(int, int, boolean) call on b1
            b1.set(rangeStart, rangeEnd, setOrClear);

            // Use a loop on b2
            for (int x = rangeStart; x < rangeEnd; x++)
                b2.set(x, setOrClear);

            // Verify their equality
            if (!b1.equals(b2)) {
                failCount++;
            }
            checkEquality(b1, b2);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testGet() {
        int failCount = 0;

        for (int i = 0; i < 1000; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Make a fairly random bitset
            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++)
                b1.set(generator.nextInt(highestPossibleSetBit));

            // Get a new set from a random range
            int rangeStart = generator.nextInt(100);
            int rangeEnd = rangeStart + generator.nextInt(100);

            RoaringBitSet b2 = (RoaringBitSet) b1.get(rangeStart, rangeEnd);

            RoaringBitSet b3 = new RoaringBitSet();
            for (int x = rangeStart; x < rangeEnd; x++)
                b3.set(x - rangeStart, b1.get(x));

            // Verify their equality
            if (!b2.equals(b3)) {
                failCount++;
            }
            checkEquality(b2, b3);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testAndNot() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            // Set some random bits in first set and remember them
            int nextBitToSet = 0;
            for (int x = 0; x < 10; x++)
                b1.set(generator.nextInt(255));

            // Set some random bits in second set and remember them
            for (int x = 10; x < 20; x++)
                b2.set(generator.nextInt(255));

            // andNot the sets together
            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            b3.andNot(b2);

            // Examine each bit of b3 for errors
            for (int x = 0; x < 256; x++) {
                boolean bit1 = b1.get(x);
                boolean bit2 = b2.get(x);
                boolean bit3 = b3.get(x);
                if (!(bit3 == (bit1 & (!bit2))))
                    failCount++;
            }
            checkSanity(b1, b2, b3);
        }
        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testAnd() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            // Set some random bits in first set and remember them
            int nextBitToSet = 0;
            for (int x = 0; x < 10; x++)
                b1.set(generator.nextInt(255));

            // Set more random bits in second set and remember them
            for (int x = 10; x < 20; x++)
                b2.set(generator.nextInt(255));

            // And the sets together
            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            b3.and(b2);

            // Examine each bit of b3 for errors
            for (int x = 0; x < 256; x++) {
                boolean bit1 = b1.get(x);
                boolean bit2 = b2.get(x);
                boolean bit3 = b3.get(x);
                if (!(bit3 == (bit1 & bit2)))
                    failCount++;
            }
            checkSanity(b1, b2, b3);
        }

        // `and' that happens to clear the last word
        RoaringBitSet b4 = makeSet(2, 127);
        b4.and(makeSet(2, 64));
        checkSanity(b4);
        if (!(b4.equals(makeSet(2))))
            failCount++;

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testOr() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();
            int[] history = new int[20];

            // Set some random bits in first set and remember them
            int nextBitToSet = 0;
            for (int x = 0; x < 10; x++) {
                nextBitToSet = generator.nextInt(255);
                history[x] = nextBitToSet;
                b1.set(nextBitToSet);
            }

            // Set more random bits in second set and remember them
            for (int x = 10; x < 20; x++) {
                nextBitToSet = generator.nextInt(255);
                history[x] = nextBitToSet;
                b2.set(nextBitToSet);
            }

            // Or the sets together
            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            b3.or(b2);

            // Verify the set bits of b3 from the history
            int historyIndex = 0;
            for (int x = 0; x < 20; x++) {
                if (!b3.get(history[x]))
                    failCount++;
            }

            // Examine each bit of b3 for errors
            for (int x = 0; x < 256; x++) {
                boolean bit1 = b1.get(x);
                boolean bit2 = b2.get(x);
                boolean bit3 = b3.get(x);
                if (!(bit3 == (bit1 | bit2)))
                    failCount++;
            }
            checkSanity(b1, b2, b3);
        }
        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testXor() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            // Set some random bits in first set and remember them
            int nextBitToSet = 0;
            for (int x = 0; x < 10; x++)
                b1.set(generator.nextInt(255));

            // Set more random bits in second set and remember them
            for (int x = 10; x < 20; x++)
                b2.set(generator.nextInt(255));

            // Xor the sets together
            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            b3.xor(b2);

            // Examine each bit of b3 for errors
            for (int x = 0; x < 256; x++) {
                boolean bit1 = b1.get(x);
                boolean bit2 = b2.get(x);
                boolean bit3 = b3.get(x);
                if (!(bit3 == (bit1 ^ bit2)))
                    failCount++;
            }
            checkSanity(b1, b2, b3);
            b3.xor(b3);
            checkEmpty(b3);
        }

        // xor that happens to clear the last word
        RoaringBitSet b4 = makeSet(2, 64, 127);
        b4.xor(makeSet(64, 127));
        checkSanity(b4);
        if (!(b4.equals(makeSet(2))))
            failCount++;

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testLength() {
        int failCount = 0;

        // Test length after set
        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            int highestSetBit = 0;

            for (int x = 0; x < 100; x++) {
                int nextBitToSet = generator.nextInt(255);
                if (nextBitToSet > highestSetBit)
                    highestSetBit = nextBitToSet;
                b1.set(nextBitToSet);
                if (b1.length() != highestSetBit + 1)
                    failCount++;
            }
            checkSanity(b1);
        }

        // Test length after flip
        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            for (int x = 0; x < 100; x++) {
                // Flip a random range twice
                int rangeStart = generator.nextInt(100);
                int rangeEnd = rangeStart + generator.nextInt(100);
                b1.flip(rangeStart);
                b1.flip(rangeStart);
                if (b1.length() != 0)
                    failCount++;
                b1.flip(rangeStart, rangeEnd);
                b1.flip(rangeStart, rangeEnd);
                if (b1.length() != 0)
                    failCount++;
            }
            checkSanity(b1);
        }

        // Test length after or
        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();
            int bit1 = generator.nextInt(100);
            int bit2 = generator.nextInt(100);
            int highestSetBit = (bit1 > bit2) ? bit1 : bit2;
            b1.set(bit1);
            b2.set(bit2);
            b1.or(b2);
            if (b1.length() != highestSetBit + 1)
                failCount++;
            checkSanity(b1, b2);
        }
        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testEquals() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            // Create BitSets of different sizes
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            // Set some random bits
            int nextBitToSet = 0;
            for (int x = 0; x < 10; x++) {
                nextBitToSet += generator.nextInt(50) + 1;
                b1.set(nextBitToSet);
                b2.set(nextBitToSet);
            }

            // Verify their equality despite different storage sizes
            if (!b1.equals(b2))
                failCount++;
            checkEquality(b1, b2);
        }
        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testNextSetBit() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            int numberOfSetBits = generator.nextInt(100) + 1;
            RoaringBitSet testSet = new RoaringBitSet();
            int[] history = new int[numberOfSetBits];

            // Set some random bits and remember them
            int nextBitToSet = 0;
            for (int x = 0; x < numberOfSetBits; x++) {
                nextBitToSet += generator.nextInt(30) + 1;
                history[x] = nextBitToSet;
                testSet.set(nextBitToSet);
            }

            // Verify their retrieval using nextSetBit()
            int historyIndex = 0;
            for (int x = testSet.nextSetBit(0); x >= 0; x = testSet.nextSetBit(x + 1)) {
                if (x != history[historyIndex++])
                    failCount++;
            }

            checkSanity(testSet);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testNextClearBit() {
        int failCount = 0;

        for (int i = 0; i < 1000; i++) {
            RoaringBitSet b = new RoaringBitSet();
            int[] history = new int[10];

            // Set all the bits
            for (int x = 0; x < 256; x++)
                b.set(x);

            // Clear some random bits and remember them
            int nextBitToClear = 0;
            for (int x = 0; x < 10; x++) {
                nextBitToClear += generator.nextInt(24) + 1;
                history[x] = nextBitToClear;
                b.clear(nextBitToClear);
            }

            // Verify their retrieval using nextClearBit()
            int historyIndex = 0;
            for (int x = b.nextClearBit(0); x < 256; x = b.nextClearBit(x + 1)) {
                if (x != history[historyIndex++])
                    failCount++;
            }

            checkSanity(b);
        }

        // regression test for 4350178
        RoaringBitSet bs = new RoaringBitSet();
        if (bs.nextClearBit(0) != 0)
            failCount++;
        for (int i = 0; i < 64; i++) {
            bs.set(i);
            if (bs.nextClearBit(0) != i + 1)
                failCount++;
        }

        checkSanity(bs);
        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testIntersects() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            // Set some random bits in first set
            int nextBitToSet = 0;
            for (int x = 0; x < 30; x++) {
                nextBitToSet = generator.nextInt(255);
                b1.set(nextBitToSet);
            }

            // Set more random bits in second set
            for (int x = 0; x < 30; x++) {
                nextBitToSet = generator.nextInt(255);
                b2.set(nextBitToSet);
            }

            // Make sure they intersect
            nextBitToSet = generator.nextInt(255);
            b1.set(nextBitToSet);
            b2.set(nextBitToSet);

            if (!b1.intersects(b2))
                failCount++;

            // Remove the common set bits
            b1.andNot(b2);

            // Make sure they don't intersect
            if (b1.intersects(b2))
                failCount++;

            checkSanity(b1, b2);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testCardinality() {
        int failCount = 0;

        for (int i = 0; i < 100; i++) {
            RoaringBitSet b1 = new RoaringBitSet();

            // Set a random number of increasing bits
            int nextBitToSet = 0;
            int iterations = generator.nextInt(20) + 1;
            for (int x = 0; x < iterations; x++) {
                nextBitToSet += generator.nextInt(20) + 1;
                b1.set(nextBitToSet);
            }

            if (b1.cardinality() != iterations) {
                failCount++;
            }

            checkSanity(b1);
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testEmpty() {
        int failCount = 0;

        RoaringBitSet b1 = new RoaringBitSet();
        if (!b1.isEmpty())
            failCount++;

        int nextBitToSet = 0;
        int numberOfSetBits = generator.nextInt(100) + 1;
        int highestPossibleSetBit = generator.nextInt(1000) + 1;
        for (int x = 0; x < numberOfSetBits; x++) {
            nextBitToSet = generator.nextInt(highestPossibleSetBit);
            b1.set(nextBitToSet);
            if (b1.isEmpty())
                failCount++;
            b1.clear(nextBitToSet);
            if (!b1.isEmpty())
                failCount++;
        }

        Assert.assertEquals(failCount, 0);
    }

    @Test
    public void testEmpty2() {
        {
            RoaringBitSet t = new RoaringBitSet();
            t.set(100);
            t.clear(3, 600);
            checkEmpty(t);
        }
        checkEmpty(new RoaringBitSet());

        RoaringBitSet s = new RoaringBitSet();
        checkEmpty(s);
        s.clear(92);
        checkEmpty(s);
        s.clear(127, 127);
        checkEmpty(s);
        s.set(127, 127);
        checkEmpty(s);
        s.set(128, 128);
        checkEmpty(s);
        RoaringBitSet empty = new RoaringBitSet();
        {
            RoaringBitSet t = new RoaringBitSet();
            t.and(empty);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.or(empty);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.xor(empty);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.andNot(empty);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.and(t);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.or(t);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.xor(t);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.andNot(t);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.and(makeSet(1));
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.and(makeSet(127));
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.and(makeSet(128));
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            t.flip(7);
            t.flip(7);
            checkEmpty(t);
        }
        {
            RoaringBitSet t = new RoaringBitSet();
            checkEmpty((RoaringBitSet) t.get(200, 300));
        }
        {
            RoaringBitSet t = makeSet(2, 5);
            Assert.assertEquals(makeSet(0, 3), t.get(2, 6));
        }
    }

    @Test
    public void testLogicalIdentities() {
        int failCount = 0;

        // Verify that (!b1)|(!b2) == !(b1&b2)
        for (int i = 0; i < 50; i++) {
            // Construct two fairly random bitsets
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++) {
                b1.set(generator.nextInt(highestPossibleSetBit));
                b2.set(generator.nextInt(highestPossibleSetBit));
            }

            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            RoaringBitSet b4 = (RoaringBitSet) b2.clone();

            for (int x = 0; x < highestPossibleSetBit; x++) {
                b1.flip(x);
                b2.flip(x);
            }
            b1.or(b2);
            b3.and(b4);
            for (int x = 0; x < highestPossibleSetBit; x++)
                b3.flip(x);
            if (!b1.equals(b3))
                failCount++;
            checkSanity(b1, b2, b3, b4);
        }

        // Verify that (b1&(!b2)|(b2&(!b1) == b1^b2
        for (int i = 0; i < 50; i++) {
            // Construct two fairly random bitsets
            RoaringBitSet b1 = new RoaringBitSet();
            RoaringBitSet b2 = new RoaringBitSet();

            int numberOfSetBits = generator.nextInt(100) + 1;
            int highestPossibleSetBit = generator.nextInt(1000) + 1;

            for (int x = 0; x < numberOfSetBits; x++) {
                b1.set(generator.nextInt(highestPossibleSetBit));
                b2.set(generator.nextInt(highestPossibleSetBit));
            }

            RoaringBitSet b3 = (RoaringBitSet) b1.clone();
            RoaringBitSet b4 = (RoaringBitSet) b2.clone();
            RoaringBitSet b5 = (RoaringBitSet) b1.clone();
            RoaringBitSet b6 = (RoaringBitSet) b2.clone();

            for (int x = 0; x < highestPossibleSetBit; x++)
                b2.flip(x);
            b1.and(b2);
            for (int x = 0; x < highestPossibleSetBit; x++)
                b3.flip(x);
            b3.and(b4);
            b1.or(b3);
            b5.xor(b6);
            if (!b1.equals(b5))
                failCount++;
            checkSanity(b1, b2, b3, b4, b5, b6);
        }
        Assert.assertEquals(failCount, 0);
    }

    private static void checkSanity(RoaringBitSet... sets) {
        for (RoaringBitSet s : sets) {
            int len = s.length();
            int cardinality1 = s.cardinality();
            int cardinality2 = 0;
            for (int i = s.nextSetBit(0); i >= 0; i = s.nextSetBit(i + 1)) {
                Assert.assertTrue(s.get(i));
                cardinality2++;
            }
            Assert.assertEquals(s.nextSetBit(len), -1);
            Assert.assertEquals(len, s.nextClearBit(len));
            Assert.assertEquals((len == 0), s.isEmpty());
            Assert.assertEquals(cardinality2, cardinality1);
            Assert.assertTrue(len <= s.size());
            Assert.assertTrue(len >= 0);
            Assert.assertTrue(cardinality1 >= 0);
        }
    }

    private static void checkEquality(RoaringBitSet s, RoaringBitSet t) {
        checkSanity(s, t);
        Assert.assertEquals(t, s);
        Assert.assertEquals(t.toString(), s.toString());
        Assert.assertEquals(s.length(), t.length());
        Assert.assertEquals(s.cardinality(), t.cardinality());
    }

    private static RoaringBitSet makeSet(int... elts) {
        RoaringBitSet s = new RoaringBitSet();
        for (int elt : elts)
            s.set(elt);
        return s;
    }

    private static void checkEmpty(RoaringBitSet s) {
        Assert.assertTrue(s.isEmpty());
        Assert.assertEquals(s.length(), 0);
        Assert.assertEquals(s.cardinality(), 0);
        Assert.assertEquals(s, new RoaringBitSet());
        Assert.assertEquals(s.nextSetBit(0), -1);
        Assert.assertEquals(s.nextSetBit(127), -1);
        Assert.assertEquals(s.nextSetBit(128), -1);
        Assert.assertEquals(s.nextClearBit(0), 0);
        Assert.assertEquals(s.nextClearBit(127), 127);
        Assert.assertEquals(s.nextClearBit(128), 128);
        Assert.assertEquals(s.toString(), "{}");
        Assert.assertFalse(s.get(0));
    }
}
