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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.pool.TypePool;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.zeroturnaround.zip.ZipUtil;

/**
 * FunctionFilePackage is a class that represents a function package and
 * implements the ValidatableFunctionPackage interface which decouples the
 * function package from classloading.
 */
public class FunctionFilePackage implements AutoCloseable, ValidatableFunctionPackage {
    private final File file;
    private final ClassFileLocator.Compound classFileLocator;
    private final TypePool typePool;
    private final boolean isNar;
    private final String narExtractionDirectory;
    private final boolean enableClassloading;

    private ClassLoader classLoader;

    private final Object configMetadata;

    public FunctionFilePackage(File file, String narExtractionDirectory, boolean enableClassloading,
                               Class<?> configClass) {
        this.file = file;
        boolean nonZeroFile = file.isFile() && file.length() > 0;
        this.isNar = nonZeroFile ? ZipUtil.containsAnyEntry(file,
                new String[] {"META-INF/services/pulsar-io.yaml", "META-INF/bundled-dependencies"}) : false;
        this.narExtractionDirectory = narExtractionDirectory;
        this.enableClassloading = enableClassloading;
        if (isNar) {
            List<File> classpathFromArchive = null;
            try {
                classpathFromArchive = NarClassLoader.getClasspathFromArchive(file, narExtractionDirectory);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            List<ClassFileLocator> classFileLocators = new ArrayList<>();
            classFileLocators.add(ClassFileLocator.ForClassLoader.ofSystemLoader());
            for (File classpath : classpathFromArchive) {
                if (classpath.exists()) {
                    try {
                        ClassFileLocator locator;
                        if (classpath.isDirectory()) {
                            locator = new ClassFileLocator.ForFolder(classpath);
                        } else {
                            locator = ClassFileLocator.ForJarFile.of(classpath);
                        }
                        classFileLocators.add(locator);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
            this.classFileLocator = new ClassFileLocator.Compound(classFileLocators);
            this.typePool = TypePool.Default.of(classFileLocator);
            try {
                this.configMetadata = FunctionUtils.getPulsarIOServiceConfig(file, configClass);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            try {
                this.classFileLocator = nonZeroFile
                        ? new ClassFileLocator.Compound(ClassFileLocator.ForClassLoader.ofSystemLoader(),
                                ClassFileLocator.ForJarFile.of(file)) :
                        new ClassFileLocator.Compound(ClassFileLocator.ForClassLoader.ofSystemLoader());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            this.typePool =
                    TypePool.Default.of(classFileLocator);
            this.configMetadata = null;
        }
    }

    public TypeDescription resolveType(String className) {
        return typePool.describe(className).resolve();
    }

    public boolean isNar() {
        return isNar;
    }

    public File getFile() {
        return file;
    }

    public TypePool getTypePool() {
        return typePool;
    }

    @Override
    public <T> T getFunctionMetaData(Class<T> clazz) {
        return configMetadata != null ? clazz.cast(configMetadata) : null;
    }

    @Override
    public synchronized void close() throws IOException {
        classFileLocator.close();
        if (classLoader instanceof Closeable) {
            ((Closeable) classLoader).close();
        }
    }

    public boolean isEnableClassloading() {
        return enableClassloading;
    }

    public synchronized ClassLoader getClassLoader() {
        if (classLoader == null) {
            classLoader = createClassLoader();
        }
        return classLoader;
    }

    private ClassLoader createClassLoader() {
        if (enableClassloading) {
            if (isNar) {
                try {
                    return NarClassLoaderBuilder.builder()
                            .narFile(file)
                            .extractionDirectory(narExtractionDirectory)
                            .build();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                try {
                    return new URLClassLoader(new java.net.URL[] {file.toURI().toURL()},
                            NarClassLoader.class.getClassLoader());
                } catch (MalformedURLException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } else {
            throw new IllegalStateException("Classloading is not enabled");
        }
    }

    @Override
    public String toString() {
        return "FunctionFilePackage{"
                + "file=" + file
                + ", isNar=" + isNar
                + '}';
    }
}
