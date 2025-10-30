/*
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
package org.apache.pulsar.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.pool.TypePool;

/**
 * Utility class to filter TestNG tests based on groups and generate
 * Maven Surefire includes/excludes files.
 *
 * This tool parses Java test class files using ByteBuddy to extract TestNG
 * Test annotations and their groups, then generates two files:
 * - includes file: Contains test classes/methods that match the specified groups
 * - excludes file: Contains test methods to exclude from classes that have matching tests
 *
 * The tests must be compiled before running this tool. There are "testFilterCreate" and "testFiltering" profiles
 * in apache/pulsar pom.xml that are used together with this tool.
 *
 * Creating includes/excludes files for "some-group" and "some-group2"
 * mvn -PtestFilterCreate exec:exec -Dgroups=some-group,some-group2
 *
 * After this, it's possible to run tests with:
 * mvn -PtestFiltering test
 *
 * Commonly the tests are targeted to run for specific modules by passing "-pl module1[,module2,module3,...]" to both
 * commands.
 */
public class TestNGTestFilter {

    private static final String TEST_ANNOTATION = "org.testng.annotations.Test";

    /**
     * Filters TestNG tests based on groups and generates include/exclude files.
     *
     * @param testClassDirectory Directory containing compiled test class files
     * @param includedGroups Set of groups to include in the test run
     * @param includesOutputFile Path to the output file for included tests
     * @param excludesOutputFile Path to the output file for excluded tests
     * @throws IOException if there's an error reading class files or writing output files
     */
    public static void generateTestFilters(
            String testClassDirectory,
            Set<String> includedGroups,
            String includesOutputFile,
            String excludesOutputFile) throws IOException {

        Path classDir = Paths.get(testClassDirectory);

        if (!Files.exists(classDir) || !Files.isDirectory(classDir)) {
            throw new IllegalArgumentException(
                    "Invalid test class directory: " + testClassDirectory + " absolute path: "
                            + classDir.toAbsolutePath());
        }

        List<String> includes = new ArrayList<>();
        List<String> excludes = new ArrayList<>();

        // Find all .class files recursively
        try (Stream<Path> paths = Files.walk(classDir)) {
            List<Path> classFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".class"))
                    .collect(Collectors.toList());

            // Create TypePool for class file analysis
            TypePool typePool = TypePool.Default.ofSystemLoader();

            for (Path classFile : classFiles) {
                processClassFile(classFile, classDir, typePool, includedGroups, includes, excludes);
            }
        }

        // Write output files
        if (includes.isEmpty()) {
            includes.add("");
            excludes.add("**/*");
        }
        writeOutputFile(includesOutputFile, includes);
        writeOutputFile(excludesOutputFile, excludes);
    }

    /**
     * Process a single class file to determine which tests to include/exclude.
     */
    private static void processClassFile(
            Path classFile,
            Path baseDir,
            TypePool typePool,
            Set<String> includedGroups,
            List<String> includes,
            List<String> excludes) {

        try {
            // Convert file path to fully qualified class name
            String relativePath = baseDir.relativize(classFile).toString();
            String className = relativePath
                    .replace(File.separatorChar, '.')
                    .replace(".class", "");

            TypeDescription typeDescription = typePool.describe(className).resolve();

            // Get class-level @Test annotation groups
            Set<String> classLevelGroups = getClassLevelGroups(typeDescription);

            // Track methods that match and don't match
            List<String> matchingMethods = new ArrayList<>();
            List<String> nonMatchingMethods = new ArrayList<>();

            Set<String> handledMethods = new HashSet<>();
            // Check each method for @Test annotation
            TypeDescription currentType = typeDescription;
            while (currentType != null) {
                for (MethodDescription.InDefinedShape method : currentType.getDeclaredMethods()) {
                    if (isTestMethod(method) && !handledMethods.contains(method.getName())) {
                        Set<String> methodGroups = getTestGroups(method.getDeclaredAnnotations());

                        // Merge class-level and method-level groups
                        Set<String> effectiveGroups = new HashSet<>();
                        if (!methodGroups.isEmpty()) {
                            effectiveGroups.addAll(methodGroups);
                        } else if (!classLevelGroups.isEmpty()) {
                            effectiveGroups.addAll(classLevelGroups);
                        } else {
                            effectiveGroups.add("_default");
                        }

                        // Check if any group matches included groups
                        boolean matches = effectiveGroups.stream().anyMatch(includedGroups::contains);

                        if (matches) {
                            matchingMethods.add(method.getName());
                        } else {
                            nonMatchingMethods.add(method.getName());
                        }
                        handledMethods.add(method.getName());
                    }
                }
                currentType = currentType.getSuperClass() != null ? currentType.getSuperClass().asErasure() : null;
            }

            // Determine output based on matching methods
            if (!matchingMethods.isEmpty()) {
                if (classLevelGroups.stream().anyMatch(includedGroups::contains)
                        || classLevelGroups.isEmpty() && includedGroups.contains("_default")) {
                    // Include entire class
                    includes.add(className);
                    // Exclude specific methods
                    for (String method : nonMatchingMethods) {
                        excludes.add(className + "#" + method);
                    }
                } else {
                    // Some methods match - include specific methods
                    for (String method : matchingMethods) {
                        includes.add(className + "#" + method);
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Warning: Could not process class file " + classFile + ": " + e.getMessage());
        }
    }

    private static Set<String> getClassLevelGroups(TypeDescription typeDescription) {
        Set<String> classLevelGroups = getTestGroups(typeDescription.getDeclaredAnnotations());
        TypeDescription.Generic superClass = typeDescription.getSuperClass();
        while (classLevelGroups.isEmpty() && superClass != null) {
            classLevelGroups = getTestGroups(superClass.asErasure().getDeclaredAnnotations());
            superClass = superClass.getSuperClass();
        }
        return classLevelGroups;
    }

    /**
     * Check if a method has @Test annotation.
     */
    private static boolean isTestMethod(MethodDescription method) {
        for (AnnotationDescription annotation : method.getDeclaredAnnotations()) {
            if (annotation.getAnnotationType().getName().equals(TEST_ANNOTATION)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extract groups from @Test annotation.
     */
    private static Set<String> getTestGroups(Iterable<AnnotationDescription> annotations) {
        Set<String> groups = new HashSet<>();

        for (AnnotationDescription annotation : annotations) {
            if (annotation.getAnnotationType().getName().equals(TEST_ANNOTATION)) {
                // Get the groups attribute
                Object groupsValue = annotation.getValue("groups").resolve();
                if (groupsValue instanceof String[]) {
                    groups.addAll(Arrays.asList((String[]) groupsValue));
                } else if (groupsValue instanceof String) {
                    groups.add((String) groupsValue);
                }
            }
        }

        return groups;
    }

    /**
     * Write lines to output file.
     */
    private static void writeOutputFile(String filePath, List<String> lines) throws IOException {
        Path path = Paths.get(filePath);

        // Create parent directories if they don't exist
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        Files.write(path, lines);
    }

    /**
     * Example usage of the utility.
     */
    public static void main(String[] args) {
        Set<String> includedGroups;
        String testClassDirectory = "target/test-classes";
        String includesOutputFile = "target/surefire-includes.txt";
        String excludesOutputFile = "target/surefire-excludes.txt";

        if (args.length == 0) {
            System.out.println(
                    "Usage: TestNGTestFilter <included groups> <testClassDirectory> <includesOutputFile> "
                            + "<excludesOutputFile>");
            System.exit(1);
        }

        includedGroups = new HashSet<>(Arrays.asList(args[0].split(",")));
        if (args.length > 1) {
            testClassDirectory = args[1];
        }
        if (args.length > 2) {
            includesOutputFile = args[2];
        }
        if (args.length > 3) {
            excludesOutputFile = args[3];
        }

        try {
            generateTestFilters(testClassDirectory, includedGroups, includesOutputFile, excludesOutputFile);
            System.out.println("Test filter files generated successfully:");
            System.out.println("  Includes: " + includesOutputFile);
            System.out.println("  Excludes: " + excludesOutputFile);
        } catch (IOException e) {
            System.err.println("Error generating test filters: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}