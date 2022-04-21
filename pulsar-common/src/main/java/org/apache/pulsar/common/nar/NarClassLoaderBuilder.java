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

/**
 * This class was adapted from NiFi NAR Utils
 * https://github.com/apache/nifi/tree/master/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-nar-utils
 */
package org.apache.pulsar.common.nar;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

/**
 * NarClassLoader builder class.
 */
public class NarClassLoaderBuilder {

    private File narFile;
    private Set<String> additionalJars;
    private ClassLoader parentClassLoader;
    private String extractionDirectory;

    public static NarClassLoaderBuilder builder() {
        return new NarClassLoaderBuilder();
    }

    public NarClassLoaderBuilder narFile(File narFile) {
        this.narFile = narFile;
        return this;
    }

    public NarClassLoaderBuilder additionalJars(Set<String> additionalJars) {
        this.additionalJars = additionalJars;
        return this;
    }

    public NarClassLoaderBuilder parentClassLoader(ClassLoader parentClassLoader) {
        this.parentClassLoader = parentClassLoader;
        return this;
    }
    public NarClassLoaderBuilder extractionDirectory(String extractionDirectory) {
        this.extractionDirectory = extractionDirectory;
        return this;
    }

    public NarClassLoader build() throws IOException {
        if (parentClassLoader == null) {
            parentClassLoader = NarClassLoader.class.getClassLoader();
        }
        if (extractionDirectory == null) {
            extractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;
        }
        Objects.requireNonNull(narFile);
        return NarClassLoader.getFromArchive(narFile, additionalJars, parentClassLoader, extractionDirectory);
    }

}
