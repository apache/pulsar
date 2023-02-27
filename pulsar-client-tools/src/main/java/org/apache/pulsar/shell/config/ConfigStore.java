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
package org.apache.pulsar.shell.config;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Shell configurations store layer.
 */
public interface ConfigStore {

    String DEFAULT_CONFIG = "default";
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    class ConfigEntry {
        String name;
        String value;
    }


    void putConfig(ConfigEntry entry) throws IOException;

    ConfigEntry getConfig(String name) throws IOException;

    void deleteConfig(String name) throws IOException;

    List<ConfigEntry> listConfigs() throws IOException;

    void setLastUsed(String name) throws IOException;

    ConfigEntry getLastUsed() throws IOException;

    static void cleanupValue(ConfigEntry entry) {
        StringBuilder builder = new StringBuilder();
        try (Scanner scanner = new Scanner(entry.getValue());) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                builder.append(line);
                builder.append(System.lineSeparator());
            }
        }
        entry.setValue(builder.toString());
    }

    static void setProperty(ConfigEntry entry, String propertyName, String propertyValue) {
        Set<String> keys = new HashSet<>();
        StringBuilder builder = new StringBuilder();
        try (Scanner scanner = new Scanner(entry.getValue());) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                final String[] split = line.split("=", 2);
                if (split.length > 0) {
                    final String property = split[0];
                    if (!keys.add(property)) {
                        continue;
                    }
                    if (property.equals(propertyName)) {
                        line = property + "=" + propertyValue;
                    }
                }
                builder.append(line);
                builder.append(System.lineSeparator());
            }
            if (!keys.contains(propertyName)) {
                builder.append(propertyName + "=" + propertyValue);
                builder.append(System.lineSeparator());
            }
        }
        entry.setValue(builder.toString());
    }

    static String getProperty(ConfigEntry entry, String propertyName) {
        try (Scanner scanner = new Scanner(entry.getValue());) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                final String[] split = line.split("=", 2);
                if (split.length > 0) {
                    final String property = split[0];
                    if (property.equals(propertyName)) {
                        if (split.length > 1) {
                            return split[1];
                        }
                        return null;
                    }
                }
            }
        }
        return null;
    }


}
