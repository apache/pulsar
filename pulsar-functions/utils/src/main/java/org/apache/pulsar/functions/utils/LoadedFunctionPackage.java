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

import java.io.IOException;
import java.io.UncheckedIOException;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.pool.TypePool;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;

/**
 * LoadedFunctionPackage is a class that represents a function package and
 * implements the ValidatableFunctionPackage interface which decouples the
 * function package from classloading. This implementation is backed by
 * a ClassLoader, and it is used when the function package is already loaded
 * by a ClassLoader. This is the case in the LocalRunner and in some of
 * the unit tests.
 */
public class LoadedFunctionPackage implements ValidatableFunctionPackage {
    private final ClassLoader classLoader;
    private final Object configMetadata;
    private final TypePool typePool;

    public <T> LoadedFunctionPackage(ClassLoader classLoader, Class<T> configMetadataClass, T configMetadata) {
        this.classLoader = classLoader;
        this.configMetadata = configMetadata;
        typePool = TypePool.Default.of(
                ClassFileLocator.ForClassLoader.of(classLoader));
    }

    public LoadedFunctionPackage(ClassLoader classLoader, Class<?> configMetadataClass) {
        this.classLoader = classLoader;
        if (classLoader instanceof NarClassLoader) {
            try {
                configMetadata = FunctionUtils.getPulsarIOServiceConfig((NarClassLoader) classLoader,
                        configMetadataClass);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            configMetadata = null;
        }
        typePool = TypePool.Default.of(
                ClassFileLocator.ForClassLoader.of(classLoader));
    }

    @Override
    public TypeDescription resolveType(String className) {
        return typePool.describe(className).resolve();
    }

    @Override
    public TypePool getTypePool() {
        return typePool;
    }

    @Override
    public <T> T getFunctionMetaData(Class<T> clazz) {
        return configMetadata != null ? clazz.cast(configMetadata) : null;
    }

    @Override
    public boolean isEnableClassloading() {
        return true;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
