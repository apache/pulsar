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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionCacheManagerImpl}.
 */
public class FunctionCacheManagerImplTest {

    private URL jarUrl;
    private List<String> jarFiles;
    private List<URL> classpaths;
    private FunctionCacheManagerImpl cacheManager;

    @BeforeMethod
    public void setUp() {
        this.jarUrl = getClass().getClassLoader().getResource("multifunction.jar");
        this.jarFiles = new ArrayList<>(Collections.singletonList(jarUrl.getPath()));
        this.classpaths = Collections.emptyList();
        this.cacheManager = new FunctionCacheManagerImpl();
    }

    @AfterMethod
    public void tearDown() {
        this.cacheManager.close();
    }

    void verifyClassLoader(ClassLoader clsLoader) throws Exception {
        assertNotNull(clsLoader);
        Class<? extends Function<Integer, Integer>> cls =
            (Class<? extends Function<Integer, Integer>>)
                clsLoader.loadClass("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        Function<Integer, Integer> func = cls.newInstance();
        assertEquals(4, func.apply(2).intValue());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testGetClassLoaderNullFunctionID() {
        this.cacheManager.getClassLoader(null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testGetClassLoaderNotFound() {
        this.cacheManager.getClassLoader(java.util.UUID.randomUUID().toString());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testRegisterNullFunctionID() throws Exception {
        this.cacheManager.registerFunctionInstance(
            null,
                java.util.UUID.randomUUID().toString(),
            Collections.emptyList(),
            Collections.emptyList());
    }

    @Test
    public void testRegister() throws Exception {
        String fid = java.util.UUID.randomUUID().toString();
        String eid = java.util.UUID.randomUUID().toString();
        this.cacheManager.registerFunctionInstance(fid, eid,
            jarFiles,
            classpaths);

        assertEquals(1, cacheManager.getCacheFunctions().size());
        FunctionCacheEntry entry = cacheManager.getCacheFunctions().get(fid);
        assertNotNull(entry);
        assertTrue(entry.isInstanceRegistered(eid));
        verifyClassLoader(cacheManager.getClassLoader(fid));
    }

    @Test
    public void testRegisterTwoInstances() throws Exception {
        String fid = java.util.UUID.randomUUID().toString();
        String iid1 = java.util.UUID.randomUUID().toString();
        String iid2 = java.util.UUID.randomUUID().toString();

        this.cacheManager.registerFunctionInstance(
            fid,
            iid1,
            jarFiles,
            classpaths);

        assertEquals(1, cacheManager.getCacheFunctions().size());
        FunctionCacheEntry entry1 = cacheManager.getCacheFunctions().get(fid);
        assertNotNull(entry1);
        assertTrue(entry1.isInstanceRegistered(iid1));
        verifyClassLoader(cacheManager.getClassLoader(fid));

        this.cacheManager.registerFunctionInstance(
            fid,
            iid2,
            jarFiles,
            classpaths);

        assertEquals(1, cacheManager.getCacheFunctions().size());
        FunctionCacheEntry entry2 = cacheManager.getCacheFunctions().get(fid);
        assertNotNull(entry2);
        assertSame(entry1, entry2);
        assertTrue(entry1.isInstanceRegistered(iid2));
    }

    @Test
    public void testUnregister() throws Exception {
        String fid = java.util.UUID.randomUUID().toString();
        String iid = java.util.UUID.randomUUID().toString();

        this.cacheManager.registerFunctionInstance(
            fid,
            iid,
            jarFiles,
            classpaths);

        assertEquals(1, cacheManager.getCacheFunctions().size());
        FunctionCacheEntry entry = cacheManager.getCacheFunctions().get(fid);
        assertNotNull(entry);
        assertTrue(entry.isInstanceRegistered(iid));
        verifyClassLoader(cacheManager.getClassLoader(fid));

        this.cacheManager.unregisterFunctionInstance(
            fid,
            iid);

        assertEquals(0, cacheManager.getCacheFunctions().size());
        assertNull(cacheManager.getCacheFunctions().get(fid));
        assertFalse(entry.isInstanceRegistered(iid));
    }

}
