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
package org.apache.bookkeeper.mledger.offload;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.testng.Assert.assertSame;
import java.nio.file.Paths;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OffloadersCacheTest {

    @Test
    public void testLoadsOnlyOnce() throws Exception {
        Offloaders expectedOffloaders = new Offloaders();
        String normalizedPath = Paths.get("./offloaders").toAbsolutePath().normalize().toString();

        try (MockedStatic<OffloaderUtils> offloaderUtils = Mockito.mockStatic(OffloaderUtils.class)) {
            offloaderUtils.when(() -> OffloaderUtils.searchForOffloaders(eq(normalizedPath), eq("/tmp")))
                    .thenReturn(expectedOffloaders);

            OffloadersCache cache = new OffloadersCache();

            // Call a first time to load the offloader
            Offloaders offloaders1 = cache.getOrLoadOffloaders("./offloaders", "/tmp");

            assertSame(offloaders1, expectedOffloaders, "The offloaders should be the mocked one.");

            // Call a second time to get the stored offlaoder
            Offloaders offloaders2 = cache.getOrLoadOffloaders("./offloaders", "/tmp");

            assertSame(offloaders2, expectedOffloaders, "The offloaders should be the mocked one.");
        }
    }

    @Test
    public void testEquivalentPathsLoadOnlyOnce() throws Exception {
        String relativePath = "./offloaders";
        String normalizedPath = Paths.get(relativePath).toAbsolutePath().normalize().toString();
        Offloaders expectedOffloaders = new Offloaders();

        try (MockedStatic<OffloaderUtils> offloaderUtils = Mockito.mockStatic(OffloaderUtils.class)) {
            offloaderUtils.when(() -> OffloaderUtils.searchForOffloaders(eq(relativePath), eq("/tmp")))
                    .thenReturn(expectedOffloaders);
            offloaderUtils.when(() -> OffloaderUtils.searchForOffloaders(eq(normalizedPath), eq("/tmp")))
                    .thenReturn(expectedOffloaders);

            OffloadersCache cache = new OffloadersCache();

            Offloaders offloaders1 = cache.getOrLoadOffloaders(relativePath, "/tmp");
            Offloaders offloaders2 = cache.getOrLoadOffloaders(normalizedPath, "/tmp");

            assertSame(offloaders1, expectedOffloaders, "The relative path should load the mocked offloaders.");
            assertSame(offloaders2, expectedOffloaders, "The absolute path should reuse the cached offloaders.");
            offloaderUtils.verify(
                    () -> OffloaderUtils.searchForOffloaders(eq(normalizedPath), eq("/tmp")), times(1));
            offloaderUtils.verify(
                    () -> OffloaderUtils.searchForOffloaders(eq(relativePath), eq("/tmp")), never());
        }
    }
}
