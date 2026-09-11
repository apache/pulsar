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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import lombok.CustomLog;
import org.testng.annotations.Test;

@CustomLog
public class OffsetsCacheTest {

    @Test
    public void testCache() throws Exception {
        // TTL is set to 1 second via JVM arg in build.gradle.kts to ensure it takes
        // effect before the OffsetsCache class is loaded (static field initialization).
        OffsetsCache offsetsCache = new OffsetsCache();
        assertNull(offsetsCache.getIfPresent(1, 2));
        offsetsCache.put(1, 1, 1);
        assertEquals(offsetsCache.getIfPresent(1, 1), 1);
        offsetsCache.clear();
        assertNull(offsetsCache.getIfPresent(1, 1));
        // test ttl - sleep longer than the TTL (1s) and the eviction period (max(TTL/2, 1) = 1s)
        // The cache uses expireAfterAccess, so we must NOT access the entry during the sleep.
        offsetsCache.put(1, 2, 2);
        assertEquals(offsetsCache.getIfPresent(1, 2), 2);
        Thread.sleep(5000);
        assertNull(offsetsCache.getIfPresent(1, 2));
        offsetsCache.close();
    }
}
