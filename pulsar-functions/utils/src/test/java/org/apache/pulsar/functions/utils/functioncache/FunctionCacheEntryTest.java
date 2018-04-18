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

package org.apache.pulsar.functions.utils.functioncache;

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionCacheEntry}.
 */
public class FunctionCacheEntryTest {

    private final URL jarUrl;
    private final Set<String> jarFiles;
    private final Set<URL> classpaths;
    private final URL[] libraryUrls;

    public FunctionCacheEntryTest() {
        this.jarUrl = getClass().getClassLoader().getResource("multifunction.jar");
        this.jarFiles = new HashSet<>();
        this.jarFiles.add(jarUrl.getPath());
        this.libraryUrls = new URL[] { jarUrl };
        this.classpaths = Collections.emptySet();
    }

    @Test
    public void testConstructor() {
        String iid = java.util.UUID.randomUUID().toString();
        FunctionCacheEntry entry = new FunctionCacheEntry(
            jarFiles,
            classpaths,
            libraryUrls,
            iid);
        assertTrue(entry.isInstanceRegistered(iid));
        entry.close();
    }

    @Test
    public void testUnregister() {
        String iid1 = java.util.UUID.randomUUID().toString();
        String iid2 = java.util.UUID.randomUUID().toString();
        FunctionCacheEntry entry = new FunctionCacheEntry(
            jarFiles,
            classpaths,
            libraryUrls,
            iid1);
        assertTrue(entry.isInstanceRegistered(iid1));
        assertFalse(entry.isInstanceRegistered(iid2));

        assertFalse(entry.unregister(iid2));
        assertTrue(entry.unregister(iid1));
        assertFalse(entry.isInstanceRegistered(iid1));
        entry.close();
    }

    @Test
    public void testRegisterJarFilesDontMatch() {
        String iid = java.util.UUID.randomUUID().toString();
        FunctionCacheEntry entry = new FunctionCacheEntry(
            jarFiles,
            classpaths,
            libraryUrls,
            iid);
        String iid2 = java.util.UUID.randomUUID().toString();
        try {
            entry.register(
                iid2,
                Collections.emptySet(),
                Collections.emptySet());
            fail("Should fail to register an instance if jar files don't match");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar files"));
        } finally {
            entry.close();
        }
    }

    @Test
    public void testRegisterClasspathsDontMatch() throws IOException {
        String iid = java.util.UUID.randomUUID().toString();
        FunctionCacheEntry entry = new FunctionCacheEntry(
            jarFiles,
            classpaths,
            libraryUrls,
            iid);
        String iid2 = java.util.UUID.randomUUID().toString();
        try {
            entry.register(
                iid2,
                jarFiles,
                Collections.singletonList(URI.create("http://localhost").toURL()));
            fail("Should fail to register an instance if jar files don't match");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("classpaths"));
        } finally {
            entry.close();
        }
    }

    @Test
    public void testRegister() {
        String iid1 = java.util.UUID.randomUUID().toString();
        String iid2 = java.util.UUID.randomUUID().toString();
        FunctionCacheEntry entry = new FunctionCacheEntry(
            jarFiles,
            classpaths,
            libraryUrls,
            iid1);
        assertTrue(entry.isInstanceRegistered(iid1));
        assertFalse(entry.isInstanceRegistered(iid2));

        entry.register(
            iid2,
            jarFiles,
            classpaths);
        assertTrue(entry.isInstanceRegistered(iid2));
        entry.close();
    }
}
