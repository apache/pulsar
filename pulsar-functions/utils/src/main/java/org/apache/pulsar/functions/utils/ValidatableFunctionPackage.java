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
package org.apache.pulsar.functions.utils;

import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.pool.TypePool;

/**
 * This abstraction separates the function and connector definition from classloading,
 * enabling validation without the need for classloading. It utilizes Byte Buddy for
 * type and annotation resolution.
 *
 * The function or connector definition is directly extracted from the archive file,
 * eliminating the need for classloader initialization.
 *
 * The getClassLoader method should only be invoked when classloading is enabled.
 * Classloading is required in the LocalRunner and in the Functions worker when the
 * worker is configured with the 'validateConnectorConfig' set to true.
  */
public interface ValidatableFunctionPackage {
    /**
     * Resolves the type description for the given class name within the function package.
     */
    TypeDescription resolveType(String className);
    /**
     * Returns the Byte Buddy TypePool instance for the function package.
     */
    TypePool getTypePool();
    /**
     * Returns the function or connector definition metadata.
     * Supports FunctionDefinition and ConnectorDefinition as the metadata type.
     */
    <T> T getFunctionMetaData(Class<T> clazz);
    /**
     * Returns if classloading is enabled for the function package.
     */
    boolean isEnableClassloading();
    /**
     * Returns the classloader for the function package. The classloader is
     * lazily initialized when classloading is enabled.
     */
    ClassLoader getClassLoader();
}
