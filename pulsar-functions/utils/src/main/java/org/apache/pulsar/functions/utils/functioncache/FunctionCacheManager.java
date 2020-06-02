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

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * A cache manager for caching function code and its dependencies.
 */
public interface FunctionCacheManager extends AutoCloseable {

    /**
     * Returns the function code class loader associated with id.
     *
     * @param fid function id
     * @return class loader which can load the function code.
     */
    ClassLoader getClassLoader(String fid);

    /**
     * Registers a function with its required jar files and classpaths.
     *
     * <p>The jar files are identified by their blob keys and downloaded for
     * use by a {@link ClassLoader}.
     *
     * @param fid function id
     * @param requiredJarFiles collection of blob keys identifying the required jar files.
     * @param requiredClasspaths collection of classpaths that are added to the function code class loader.
     */
    default void registerFunction(String fid,
                                  List<String> requiredJarFiles,
                                  List<URL> requiredClasspaths)
        throws IOException {
        registerFunctionInstance(fid, null, requiredJarFiles,
            requiredClasspaths);
    }

    void registerFunctionInstance(String fid,
                                  String eid,
                                  List<String> requiredJarFiles,
                                  List<URL> requiredClasspaths)
        throws IOException;

    void registerFunctionInstanceWithArchive(String fid, String eid,
                                             String narArchive,
                                             String narExtractionDirectory) throws IOException;

    /**
     * Unregisters a job from the function cache manager.
     *
     * @param fid function id
     */
    default void unregisterFunction(String fid) {
        unregisterFunctionInstance(fid, null);
    }

    void unregisterFunctionInstance(String fid,
                                    String eid);

    /**
     * Close the cache manager to release created class loaders.
     */
    @Override
    void close();
}
