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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * A <tt>ClassLoader</tt> for loading NARs (NiFi archives). NARs are designed to allow isolating bundles of code
 * (comprising one-or-more NiFi <tt>FlowFileProcessor</tt>s, <tt>FlowFileComparator</tt>s and their dependencies) from
 * other such bundles; this allows for dependencies and processors that require conflicting, incompatible versions of
 * the same dependency to run in a single instance of NiFi.
 * </p>
 *
 * <p>
 * <tt>NarClassLoader</tt> follows the delegation model described in {@link ClassLoader#findClass(java.lang.String)
 * ClassLoader.findClass(...)}; classes are first loaded from the parent <tt>ClassLoader</tt>, and only if they cannot
 * be found there does the <tt>NarClassLoader</tt> provide a definition. Specifically, this means that resources are
 * loaded from NiFi's <tt>conf</tt> and <tt>lib</tt> directories first, and if they cannot be found there, are loaded
 * from the NAR.
 * </p>
 *
 * <p>
 * The packaging of a NAR is such that it is a ZIP file with the following directory structure:
 *
 * <pre>
 *   +META-INF/
 *   +-- bundled-dependencies/
 *   +-- &lt;JAR files&gt;
 *   +-- MANIFEST.MF
 * </pre>
 * </p>
 *
 * <p>
 * The MANIFEST.MF file contains the same information as a typical JAR file but also includes two additional NiFi
 * properties: {@code Nar-Id} and {@code Nar-Dependency-Id}.
 * </p>
 *
 * <p>
 * The {@code Nar-Id} provides a unique identifier for this NAR.
 * </p>
 *
 * <p>
 * The {@code Nar-Dependency-Id} is optional. If provided, it indicates that this NAR should inherit all of the
 * dependencies of the NAR with the provided ID. Often times, the NAR that is depended upon is referred to as the
 * Parent. This is because its ClassLoader will be the parent ClassLoader of the dependent NAR.
 * </p>
 *
 * <p>
 * If a NAR is built using NiFi's Maven NAR Plugin, the {@code Nar-Id} property will be set to the artifactId of the
 * NAR. The {@code Nar-Dependency-Id} will be set to the artifactId of the NAR that is depended upon. For example, if
 * NAR A is defined as such:
 *
 * <pre>
 * ...
 * &lt;artifactId&gt;nar-a&lt;/artifactId&gt;
 * &lt;packaging&gt;nar&lt;/packaging&gt;
 * ...
 * &lt;dependencies&gt;
 *   &lt;dependency&gt;
 *     &lt;groupId&gt;group&lt;/groupId&gt;
 *     &lt;artifactId&gt;nar-z&lt;/artifactId&gt;
 *     <b>&lt;type&gt;nar&lt;/type&gt;</b>
 *   &lt;/dependency&gt;
 * &lt;/dependencies&gt;
 * </pre>
 * </p>
 *
 *
 * <p>
 * Then the MANIFEST.MF file that is created for NAR A will have the following properties set:
 * <ul>
 * <li>{@code Nar-Id: nar-a}</li>
 * <li>{@code Nar-Dependency-Id: nar-z}</li>
 * </ul>
 * </p>
 *
 * <p>
 * Note, above, that the {@code type} of the dependency is set to {@code nar}.
 * </p>
 *
 * <p>
 * If the NAR has more than one dependency of {@code type} {@code nar}, then the Maven NAR plugin will fail to build the
 * NAR.
 * </p>
 */
@Slf4j
public class NarClassLoader extends URLClassLoader {

    private static final FileFilter JAR_FILTER = pathname -> {
        final String nameToTest = pathname.getName().toLowerCase();
        return nameToTest.endsWith(".jar") && pathname.isFile();
    };

    /**
     * The NAR for which this <tt>ClassLoader</tt> is responsible.
     */
    private final File narWorkingDirectory;

    private static final String TMP_DIR_PREFIX = "pulsar-nar";

    public static final String DEFAULT_NAR_EXTRACTION_DIR = System.getProperty("java.io.tmpdir");

    static NarClassLoader getFromArchive(File narPath, Set<String> additionalJars, ClassLoader parent,
                                                String narExtractionDirectory)
        throws IOException {
        File unpacked = NarUnpacker.unpackNar(narPath, getNarExtractionDirectory(narExtractionDirectory));
        return AccessController.doPrivileged(new PrivilegedAction<NarClassLoader>() {
            @SneakyThrows
            @Override
            public NarClassLoader run() {
                return new NarClassLoader(unpacked, additionalJars, parent);
            }
        });
    }

    public static List<File> getClasspathFromArchive(File narPath, String narExtractionDirectory) throws IOException {
        File unpacked = NarUnpacker.unpackNar(narPath, getNarExtractionDirectory(narExtractionDirectory));
        return getClassPathEntries(unpacked);
    }

    private static File getNarExtractionDirectory(String configuredDirectory) {
        return new File(configuredDirectory + "/" + TMP_DIR_PREFIX);
    }

    /**
     * Construct a nar class loader.
     *
     * @param narWorkingDirectory
     *            directory to explode nar contents to
     * @param parent
     * @throws IOException
     *             if an error occurs while loading the NAR.
     */
    private NarClassLoader(final File narWorkingDirectory, Set<String> additionalJars, ClassLoader parent)
            throws IOException {
        super(new URL[0], parent);
        this.narWorkingDirectory = narWorkingDirectory;

        // process the classpath
        updateClasspath(narWorkingDirectory);

        if (additionalJars != null) {
            for (String jar : additionalJars) {
                addURL(Paths.get(jar).toUri().toURL());
            }
        }

        if (log.isDebugEnabled()) {
            log.info("Created class loader with paths: {}", Arrays.toString(getURLs()));
        }
    }

    public File getWorkingDirectory() {
        return narWorkingDirectory;
    }

    /**
     * Read a service definition as a String.
     */
    public String getServiceDefinition(String serviceName) throws IOException {
        String serviceDefPath = narWorkingDirectory + "/META-INF/services/" + serviceName;
        return new String(Files.readAllBytes(Paths.get(serviceDefPath)), StandardCharsets.UTF_8);
    }

    public List<String> getServiceImplementation(String serviceName) throws IOException {
        List<String> impls = new ArrayList<>();

        String serviceDefPath = narWorkingDirectory + "/META-INF/services/" + serviceName;

        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(serviceDefPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    final int indexOfPound = line.indexOf("#");
                    final String effectiveLine = (indexOfPound > 0) ? line.substring(0, indexOfPound) : line;
                    impls.add(effectiveLine);
                }
            }
        }

        return impls;
    }

    /**
     * Adds URLs for the resources unpacked from this NAR:
     * <ul>
     * <li>the root: for classes, <tt>META-INF</tt>, etc.</li>
     * <li><tt>META-INF/dependencies</tt>: for config files, <tt>.so</tt>s, etc.</li>
     * <li><tt>META-INF/dependencies/*.jar</tt>: for dependent libraries</li>
     * </ul>
     *
     * @param root
     *            the root directory of the unpacked NAR.
     * @throws IOException
     *             if the URL list could not be updated.
     */
    private void updateClasspath(File root) throws IOException {
        getClassPathEntries(root).forEach(f -> {
            try {
                addURL(f.toURI().toURL());
            } catch (IOException e) {
                log.error("Failed to add entry to classpath: {}", f, e);
            }
        });
    }

    static List<File> getClassPathEntries(File root) {
        List<File> classPathEntries = new ArrayList<>();
        classPathEntries.add(root);
        File dependencies = new File(root, "META-INF/bundled-dependencies");
        if (!dependencies.isDirectory()) {
            log.warn("{} does not contain META-INF/bundled-dependencies!", root);
        }
        classPathEntries.add(dependencies);
        if (dependencies.isDirectory()) {
            final File[] jarFiles = dependencies.listFiles(JAR_FILTER);
            if (jarFiles != null) {
                Arrays.sort(jarFiles, Comparator.comparing(File::getName));
                classPathEntries.addAll(Arrays.asList(jarFiles));
            }
        }
        return classPathEntries;
    }

    @Override
    protected String findLibrary(final String libname) {
        File dependencies = new File(narWorkingDirectory, "META-INF/bundled-dependencies");
        if (!dependencies.isDirectory()) {
            log.warn("{} does not contain META-INF/bundled-dependencies!", narWorkingDirectory);
        }

        final File nativeDir = new File(dependencies, "native");
        final File libsoFile = new File(nativeDir, "lib" + libname + ".so");
        final File dllFile = new File(nativeDir, libname + ".dll");
        final File soFile = new File(nativeDir, libname + ".so");
        if (libsoFile.exists()) {
            return libsoFile.getAbsolutePath();
        } else if (dllFile.exists()) {
            return dllFile.getAbsolutePath();
        } else if (soFile.exists()) {
            return soFile.getAbsolutePath();
        }

        // not found in the nar. try system native dir
        return null;
    }

    @Override
    public String toString() {
        return NarClassLoader.class.getName() + "[" + narWorkingDirectory.getPath() + "]";
    }
}
