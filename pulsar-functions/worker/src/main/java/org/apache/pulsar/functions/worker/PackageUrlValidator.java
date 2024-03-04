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
package org.apache.pulsar.functions.worker;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.pulsar.functions.proto.Function;

/**
 * Validates package URLs for functions and connectors.
 * Validates that the package URL is either a file in the connectors or functions directory
 * when referencing connector or function files is enabled, or matches one of the additional url patterns.
 */
public class PackageUrlValidator {
    private final Path connectionsDirectory;
    private final Path functionsDirectory;
    private final List<Pattern> additionalConnectionsPatterns;
    private final List<Pattern> additionalFunctionsPatterns;

    public PackageUrlValidator(WorkerConfig workerConfig) {
        this.connectionsDirectory = resolveDirectory(workerConfig.getEnableReferencingConnectorDirectoryFiles(),
                workerConfig.getConnectorsDirectory());
        this.functionsDirectory = resolveDirectory(workerConfig.getEnableReferencingFunctionsDirectoryFiles(),
                workerConfig.getFunctionsDirectory());
        this.additionalConnectionsPatterns =
                compilePatterns(workerConfig.getAdditionalEnabledConnectorUrlPatterns());
        this.additionalFunctionsPatterns =
                compilePatterns(workerConfig.getAdditionalEnabledFunctionsUrlPatterns());
    }

    private static Path resolveDirectory(Boolean enabled, String directory) {
        return enabled != null && enabled
                ? FileSystems.getDefault().getPath(directory).normalize().toAbsolutePath() : null;
    }

    private static List<Pattern> compilePatterns(List<String> additionalPatterns) {
        return additionalPatterns != null ? additionalPatterns.stream().map(Pattern::compile).collect(
                Collectors.toList()) : Collections.emptyList();
    }

    boolean isValidFunctionsPackageUrl(URI functionPkgUrl) {
        return doesMatch(functionPkgUrl, functionsDirectory, additionalFunctionsPatterns);
    }

    boolean isValidConnectionsPackageUrl(URI functionPkgUrl) {
        return doesMatch(functionPkgUrl, connectionsDirectory, additionalConnectionsPatterns);
    }

    private boolean doesMatch(URI functionPkgUrl, Path directory, List<Pattern> patterns) {
        if (directory != null && "file".equals(functionPkgUrl.getScheme())) {
            Path filePath = FileSystems.getDefault().getPath(functionPkgUrl.getPath()).normalize().toAbsolutePath();
            if (filePath.startsWith(directory)) {
                return true;
            }
        }
        String functionPkgUrlString = functionPkgUrl.normalize().toString();
        for (Pattern pattern : patterns) {
            if (pattern.matcher(functionPkgUrlString).matches()) {
                return true;
            }
        }
        return false;
    }

    public boolean isValidPackageUrl(Function.FunctionDetails.ComponentType componentType, String functionPkgUrl) {
        URI uri = URI.create(functionPkgUrl);
        if (componentType == null) {
            // if component type is not specified, we need to check both functions and connections
            return isValidFunctionsPackageUrl(uri) || isValidConnectionsPackageUrl(uri);
        }
        switch (componentType) {
            case FUNCTION:
                return isValidFunctionsPackageUrl(uri);
            case SINK:
            case SOURCE:
                return isValidConnectionsPackageUrl(uri);
            default:
                throw new IllegalArgumentException("Unknown component type: " + componentType);
        }
    }
}
