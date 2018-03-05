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
import static org.testng.Assert.assertSame;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Function;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionClassLoaders}.
 */
public class FunctionClassLoadersTest {

    @Test
    public void testCreateClassLoader() throws Exception {
        URL jarUrl = getClass().getClassLoader().getResource("multifunction.jar");
        ClassLoader parent = getClass().getClassLoader();
        URLClassLoader clsLoader = FunctionClassLoaders.create(
            new URL[] { jarUrl },
            parent);
        assertSame(parent, clsLoader.getParent());
        Class<? extends Function<Integer, Integer>> cls =
            (Class<? extends Function<Integer, Integer>>)
                clsLoader.loadClass("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
        Function<Integer, Integer> func = cls.newInstance();
        assertEquals(4, func.apply(2).intValue());
    }

    @Test(expectedExceptions = ClassNotFoundException.class)
    public void testClassNotFound() throws Exception {
        ClassLoader clsLoader = getClass().getClassLoader();
        clsLoader.loadClass("org.apache.pulsar.functions.runtime.functioncache.AddFunction");
    }

}
