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
package org.apache.pulsar.tests.performance.launcher;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Where a run writes its outputs: {@code <reports root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>/}, so that
 * every run keeps its own directory and runs can be browsed by day, branch and experiment. The run directory
 * repeats the month and day so that a copied run directory still says when it ran. Two runs of the same name
 * started within the same second would share a directory; that is not checked.
 */
final class RunDirectory {
    /** The reports root relative to the project directory when none is given. */
    static final String DEFAULT_REPORTS_ROOT = "build/performance";

    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter RUN = DateTimeFormatter.ofPattern("MM-dd-HH-mm-ss");

    private RunDirectory() {
    }

    /**
     * The directory of a run started at {@code started}, not created.
     *
     * @param branch the branch directory, from {@link #branchDirectory(String, String)}
     */
    static Path resolve(Path reportsRoot, ZonedDateTime started, String branch, String name) {
        return reportsRoot.resolve(DAY.format(started)).resolve(branch).resolve(sanitize(name))
                .resolve(RUN.format(started));
    }

    /**
     * The directory name for a git branch: {@code feat/x} is {@code feat-x}, a detached HEAD that no branch contains
     * is {@code detached-<short commit>}, and a tree without git is {@code no-git}.
     */
    static String branchDirectory(String branch, String commit) {
        if (branch.isEmpty()) {
            return "no-git";
        }
        if (branch.equals("HEAD")) {
            return "detached-" + commit.substring(0, Math.min(12, commit.length()));
        }
        return sanitize(branch);
    }

    /** A single path segment: characters other than letters, digits, '.', '_' and '-' become '-'. */
    static String sanitize(String name) {
        String sanitized = name.replaceAll("[^A-Za-z0-9._-]", "-");
        return sanitized.isEmpty() || sanitized.chars().allMatch(c -> c == '.') ? "run" : sanitized;
    }
}
