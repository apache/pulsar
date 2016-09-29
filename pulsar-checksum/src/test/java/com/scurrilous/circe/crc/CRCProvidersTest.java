/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe.crc;

import static com.scurrilous.circe.HashSupport.HARDWARE;
import static com.scurrilous.circe.HashSupport.INCREMENTAL;
import static com.scurrilous.circe.HashSupport.INT_SIZED;
import static com.scurrilous.circe.HashSupport.LONG_SIZED;
import static com.scurrilous.circe.HashSupport.NATIVE;
import static com.scurrilous.circe.HashSupport.STATEFUL;
import static com.scurrilous.circe.HashSupport.STATELESS_INCREMENTAL;
import static com.scurrilous.circe.params.CrcParameters.CRC32;
import static com.scurrilous.circe.params.CrcParameters.CRC64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.testng.annotations.Test;

import com.scurrilous.circe.HashProvider;
import com.scurrilous.circe.HashProviders;
import com.scurrilous.circe.HashSupport;
import com.scurrilous.circe.IncrementalLongHash;

@SuppressWarnings("javadoc")
public class CRCProvidersTest {

    @Test
    public void testAll() {
        final Iterator<HashProvider> i = HashProviders.iterator();
        assertTrue(i.hasNext());
        assertTrue(i.next() instanceof StandardCrcProvider);
        assertFalse(i.hasNext());
    }

    @Test
    public void testNonUnique() {
        final HashProvider provider = HashProviders.best(CRC32);
        final IncrementalLongHash i1 = provider.getIncrementalLong(CRC32);
        final IncrementalLongHash i2 = provider.getIncrementalLong(CRC32);
        assertTrue(i1 != i2);
    }

    @Test
    public void testSearchCRCParametersCRC32() {
        final SortedMap<EnumSet<HashSupport>, HashProvider> map = HashProviders.search(CRC32);
        assertEquals(1, map.size());
        final Entry<EnumSet<HashSupport>, HashProvider> entry = map.entrySet().iterator().next();
        assertEquals(EnumSet.of(NATIVE, STATELESS_INCREMENTAL, INCREMENTAL, INT_SIZED, LONG_SIZED,
                STATEFUL), entry.getKey());
        assertTrue(entry.getValue() instanceof StandardCrcProvider);
    }

    @Test
    public void testSearchCRCParametersCRC64() {
        final SortedMap<EnumSet<HashSupport>, HashProvider> map = HashProviders.search(CRC64);
        assertEquals(1, map.size());
        final Entry<EnumSet<HashSupport>, HashProvider> entry = map.entrySet().iterator().next();
        assertEquals(EnumSet.of(STATELESS_INCREMENTAL, INCREMENTAL, LONG_SIZED, STATEFUL),
                entry.getKey());
        assertTrue(entry.getValue() instanceof StandardCrcProvider);
    }

    @Test
    public void testSearchCRCParametersEnumSet() {
        assertEquals(1, HashProviders.search(CRC32, EnumSet.of(NATIVE)).size());
        assertTrue(HashProviders.search(CRC64, EnumSet.of(NATIVE)).isEmpty());
        assertTrue(HashProviders.search(CRC32, EnumSet.of(HARDWARE)).isEmpty());
    }
}
