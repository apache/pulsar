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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import java.io.File;
import java.io.IOException;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.nar.NarClassLoaderBuilder;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.utils.functions.FunctionUtils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

public class FunctionRuntimeCommon {
    public static NarClassLoader extractNarClassLoader(File packageFile,
                                                       String narExtractionDirectory) {
        if (packageFile != null) {
            try {
                return NarClassLoaderBuilder.builder()
                        .narFile(packageFile)
                        .extractionDirectory(narExtractionDirectory)
                        .build();
            } catch (IOException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        return null;
    }

    public static ClassLoader getClassLoaderFromPackage(
            Function.FunctionDetails.ComponentType componentType,
            String className,
            File packageFile,
            String narExtractionDirectory) {
        String connectorClassName = className;
        ClassLoader jarClassLoader = null;
        boolean keepJarClassLoader = false;
        NarClassLoader narClassLoader = null;
        boolean keepNarClassLoader = false;

        Exception jarClassLoaderException = null;
        Exception narClassLoaderException = null;

        try {
            try {
                jarClassLoader = ClassLoaderUtils.extractClassLoader(packageFile);
            } catch (Exception e) {
                jarClassLoaderException = e;
            }
            try {
                narClassLoader = extractNarClassLoader(packageFile, narExtractionDirectory);
            } catch (Exception e) {
                narClassLoaderException = e;
            }

            // if connector class name is not provided, we can only try to load archive as a NAR
            if (isEmpty(connectorClassName)) {
                if (narClassLoader == null) {
                    throw new IllegalArgumentException(String.format("%s package does not have the correct format. "
                                    + "Pulsar cannot determine if the package is a NAR package or JAR package. "
                                    + "%s classname is not provided and attempts to load it as a NAR package produced "
                                    + "the following error.",
                            FunctionCommon.capFirstLetter(componentType), FunctionCommon.capFirstLetter(componentType)),
                            narClassLoaderException);
                }
                try {
                    if (componentType == Function.FunctionDetails.ComponentType.FUNCTION) {
                        connectorClassName = FunctionUtils.getFunctionClass(narClassLoader);
                    } else if (componentType == Function.FunctionDetails.ComponentType.SOURCE) {
                        connectorClassName = ConnectorUtils.getIOSourceClass(narClassLoader);
                    } else {
                        connectorClassName = ConnectorUtils.getIOSinkClass(narClassLoader);
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException(String.format("Failed to extract %s class from archive",
                            componentType.toString().toLowerCase()), e);
                }

                try {
                    narClassLoader.loadClass(connectorClassName);
                    keepNarClassLoader = true;
                    return narClassLoader;
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    throw new IllegalArgumentException(String.format("%s class %s must be in class path",
                            FunctionCommon.capFirstLetter(componentType), connectorClassName), e);
                }

            } else {
                // if connector class name is provided, we need to try to load it as a JAR and as a NAR.
                if (jarClassLoader != null) {
                    try {
                        jarClassLoader.loadClass(connectorClassName);
                        keepJarClassLoader = true;
                        return jarClassLoader;
                    } catch (ClassNotFoundException | NoClassDefFoundError e) {
                        // class not found in JAR try loading as a NAR and searching for the class
                        if (narClassLoader != null) {

                            try {
                                narClassLoader.loadClass(connectorClassName);
                                keepNarClassLoader = true;
                                return narClassLoader;
                            } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                                throw new IllegalArgumentException(
                                        String.format("%s class %s must be in class path",
                                                FunctionCommon.capFirstLetter(componentType), connectorClassName), e1);
                            }
                        } else {
                            throw new IllegalArgumentException(String.format("%s class %s must be in class path",
                                    FunctionCommon.capFirstLetter(componentType), connectorClassName), e);
                        }
                    }
                } else if (narClassLoader != null) {
                    try {
                        narClassLoader.loadClass(connectorClassName);
                        keepNarClassLoader = true;
                        return narClassLoader;
                    } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                        throw new IllegalArgumentException(
                                String.format("%s class %s must be in class path",
                                        FunctionCommon.capFirstLetter(componentType), connectorClassName), e1);
                    }
                } else {
                    StringBuilder errorMsg = new StringBuilder(FunctionCommon.capFirstLetter(componentType)
                            + " package does not have the correct format."
                            + " Pulsar cannot determine if the package is a NAR package or JAR package.");

                    if (jarClassLoaderException != null) {
                        errorMsg.append(
                                " Attempts to load it as a JAR package produced error: " + jarClassLoaderException
                                        .getMessage());
                    }

                    if (narClassLoaderException != null) {
                        errorMsg.append(
                                " Attempts to load it as a NAR package produced error: " + narClassLoaderException
                                        .getMessage());
                    }

                    throw new IllegalArgumentException(errorMsg.toString());
                }
            }
        } finally {
            if (!keepJarClassLoader) {
                ClassLoaderUtils.closeClassLoader(jarClassLoader);
            }
            if (!keepNarClassLoader) {
                ClassLoaderUtils.closeClassLoader(narClassLoader);
            }
        }
    }

}
