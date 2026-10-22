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
package org.apache.pulsar.tests.performance.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves performance scenario YAML inheritance and environment overrides. */
public final class YamlScenarioLoader {
    private final ObjectMapper mapper;

    public YamlScenarioLoader() {
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public ObjectNode resolve(Path configFile, JsonNode defaults, Map<String, String> environment,
                              String environmentPrefix, String configEnvironmentName) {
        return resolve(configFile, List.of(), defaults, environment, environmentPrefix, configEnvironmentName);
    }

    /**
     * Resolves {@code configFile} with its inheritance, merges each of {@code appendedFiles} on top of it, in order and
     * with their own inheritance, as if the scenario extended them last, and applies the environment overrides.
     */
    public ObjectNode resolve(Path configFile, List<Path> appendedFiles, JsonNode defaults,
                              Map<String, String> environment, String environmentPrefix,
                              String configEnvironmentName) {
        ObjectNode root = defaults == null ? mapper.createObjectNode() : requireObject(defaults, "defaults").deepCopy();
        if (configFile != null) {
            mergeFile(root, configFile, new LinkedHashSet<>());
        }
        for (Path appendedFile : appendedFiles) {
            mergeFile(root, appendedFile, new LinkedHashSet<>());
        }
        applyEnvironmentOverrides(root, environment, environmentPrefix, configEnvironmentName);
        return root;
    }

    /**
     * Sets the value at a dotted path of a resolved scenario, {@code <path>=<value>}, such as
     * {@code workloads.iotTelemetry.rate=5000}. The path's keys match in any case, and every section on the path has
     * to exist, so that a misspelled section fails instead of adding configuration; the last key may be new, such as
     * a setting added to {@code cluster.brokers.env}. A value replacing a scalar keeps its type; any other value is
     * parsed as YAML, so that {@code [blocked]} sets a list.
     */
    public void set(ObjectNode root, String assignment) {
        int separator = assignment.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException("A setting is <path>=<value>, not '" + assignment + "'");
        }
        String[] path = assignment.substring(0, separator).split("\\.", -1);
        String value = assignment.substring(separator + 1);
        ObjectNode node = root;
        for (int i = 0; i < path.length - 1; i++) {
            String field = fieldName(node, path[i]);
            if (field == null || !(node.get(field) instanceof ObjectNode section)) {
                throw new IllegalArgumentException("Cannot set '" + assignment + "': the scenario has no section '"
                        + String.join(".", Arrays.copyOf(path, i + 1)) + "'");
            }
            node = section;
        }
        String leaf = path[path.length - 1];
        if (leaf.isEmpty()) {
            throw new IllegalArgumentException("Cannot set '" + assignment + "': the path ends with an empty key");
        }
        String field = Objects.requireNonNullElse(fieldName(node, leaf), leaf);
        JsonNode existing = node.get(field);
        node.set(field, existing != null && existing.isValueNode() && !existing.isNull()
                ? parseValue(value, existing) : parseYaml(assignment, value));
    }

    // A key's exact spelling, else the one that matches it in another case
    private static String fieldName(ObjectNode node, String key) {
        if (node.has(key)) {
            return key;
        }
        var fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            if (field.equalsIgnoreCase(key)) {
                return field;
            }
        }
        return null;
    }

    private JsonNode parseYaml(String assignment, String value) {
        if (value.isEmpty()) {
            return mapper.getNodeFactory().textNode("");
        }
        try {
            return mapper.readTree(value);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot set '" + assignment + "': the value isn't valid YAML", e);
        }
    }

    public JsonNode select(JsonNode root, String dottedPath) {
        JsonNode selected = root;
        if (dottedPath == null || dottedPath.isBlank()) {
            return selected;
        }
        for (String element : dottedPath.split("\\.")) {
            selected = selected.get(element);
            if (selected == null) {
                throw new IllegalArgumentException("Scenario path does not exist: " + dottedPath);
            }
        }
        return selected;
    }

    public void write(Path output, JsonNode resolved) {
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), resolved);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot write resolved scenario " + output, e);
        }
    }

    private void mergeFile(ObjectNode target, Path file, Set<Path> activeFiles) {
        try {
            Path path = file.toRealPath();
            if (!activeFiles.add(path)) {
                throw new IllegalArgumentException("Scenario inheritance cycle: " + activeFiles + " -> " + path);
            }
            try {
                ObjectNode source = requireObject(mapper.readTree(path.toFile()), "Scenario " + path);
                JsonNode parents = source.remove("extends");
                if (parents != null) {
                    if (parents.isTextual()) {
                        mergeParent(target, parents, path, activeFiles);
                    } else if (parents.isArray()) {
                        parents.forEach(parent -> mergeParent(target, parent, path, activeFiles));
                    } else {
                        throw new IllegalArgumentException(
                                "Scenario 'extends' must be a path or list of paths: " + path);
                    }
                }
                merge(target, source);
            } finally {
                activeFiles.remove(path);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read scenario " + file, e);
        }
    }

    private void mergeParent(ObjectNode target, JsonNode parent, Path file, Set<Path> activeFiles) {
        if (!parent.isTextual() || parent.textValue().isBlank()) {
            throw new IllegalArgumentException("Scenario 'extends' entries must be non-empty paths: " + file);
        }
        Path parentPath = Path.of(parent.textValue());
        mergeFile(target, parentPath.isAbsolute() ? parentPath : file.getParent().resolve(parentPath), activeFiles);
    }

    private static void merge(ObjectNode target, ObjectNode source) {
        source.properties().forEach(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isNull()) {
                target.remove(key);
            } else if (value instanceof ObjectNode object) {
                JsonNode current = target.get(key);
                ObjectNode child = current instanceof ObjectNode currentObject
                        ? currentObject : target.putObject(key);
                merge(child, object);
            } else {
                target.set(key, value);
            }
        });
    }

    private void applyEnvironmentOverrides(ObjectNode root, Map<String, String> environment,
                                           String prefix, String configEnvironmentName) {
        // The prefix in upper or lower case; the rest of the name matches the scenario's keys in any case
        String lowerCasePrefix = prefix.toLowerCase(Locale.ROOT);
        environment.forEach((name, value) -> {
            if (!(name.startsWith(prefix) || name.startsWith(lowerCasePrefix))
                    || name.equalsIgnoreCase(configEnvironmentName)) {
                return;
            }
            String[] path = name.substring(prefix.length()).toLowerCase(Locale.ROOT).split("_");
            ObjectNode node = root;
            int pathIndex = 0;
            while (pathIndex < path.length) {
                FieldMatch match = findField(node, path, pathIndex);
                if (match == null) {
                    return;
                }
                JsonNode existing = node.get(match.name());
                pathIndex += match.tokens();
                if (pathIndex == path.length) {
                    node.set(match.name(), parseValue(value, existing));
                    return;
                }
                if (!(existing instanceof ObjectNode object)) {
                    return;
                }
                node = object;
            }
        });
    }

    private static FieldMatch findField(ObjectNode node, String[] path, int start) {
        StringBuilder candidate = new StringBuilder();
        FieldMatch result = null;
        for (int i = start; i < path.length; i++) {
            candidate.append(path[i]);
            var fields = node.fieldNames();
            while (fields.hasNext()) {
                String field = fields.next();
                if (field.replace("_", "").equalsIgnoreCase(candidate.toString())) {
                    result = new FieldMatch(field, i - start + 1);
                }
            }
        }
        return result;
    }

    private JsonNode parseValue(String value, JsonNode existing) {
        if (existing != null && existing.isBoolean()) {
            return mapper.getNodeFactory().booleanNode(Boolean.parseBoolean(value));
        }
        if (existing != null && existing.isIntegralNumber()) {
            return mapper.getNodeFactory().numberNode(Long.parseLong(value));
        }
        if (existing != null && existing.isFloatingPointNumber()) {
            return mapper.getNodeFactory().numberNode(Double.parseDouble(value));
        }
        return mapper.getNodeFactory().textNode(value);
    }

    private static ObjectNode requireObject(JsonNode node, String description) {
        if (node instanceof ObjectNode object) {
            return object;
        }
        throw new IllegalArgumentException(description + " must be a YAML mapping");
    }

    private record FieldMatch(String name, int tokens) {
    }
}
