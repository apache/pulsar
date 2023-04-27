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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * File based configurations store.
 *
 * All the configurations are stored in a single file in JSON format.
 */
public class FileConfigStore implements ConfigStore {

    @Data
    @NoArgsConstructor
    public static class FileConfig {
        private LinkedHashMap<String, ConfigEntry> configs = new LinkedHashMap<>();
        private String last;
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final File file;
    private final ConfigEntry defaultConfig;
    private FileConfig fileConfig;

    public FileConfigStore(File file, ConfigEntry defaultConfig) throws IOException {
        this.file = file;
        if (file.exists()) {
            read();
        } else {
            fileConfig = new FileConfig();
        }
        if (defaultConfig != null) {
            this.defaultConfig = new ConfigEntry(defaultConfig.getName(), defaultConfig.getValue());
            ConfigStore.cleanupValue(this.defaultConfig);
        } else {
            this.defaultConfig = null;
        }
    }

    private void read() throws IOException {
        try (final BufferedInputStream buffered = new BufferedInputStream(new FileInputStream(file));) {
            try {
                fileConfig = mapper.readValue(buffered, FileConfig.class);
            } catch (MismatchedInputException mismatchedInputException) {
                fileConfig = new FileConfig();
            }
        }
    }

    private void write() throws IOException {
        try (final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));) {
            mapper.writeValue(bufferedOutputStream, fileConfig);
        }
    }

    @Override
    public void putConfig(ConfigEntry entry) throws IOException {
        if (DEFAULT_CONFIG.equals(entry.getName())) {
            throw new IllegalArgumentException("'" + DEFAULT_CONFIG + "' can't be modified.");
        }
        ConfigStore.cleanupValue(entry);
        fileConfig.configs.put(entry.getName(), entry);
        write();
    }

    @Override
    public ConfigEntry getConfig(String name) {
        if (DEFAULT_CONFIG.equals(name)) {
            return defaultConfig;
        }
        return fileConfig.configs.get(name);
    }

    @Override
    public void deleteConfig(String name) throws IOException{
        if (DEFAULT_CONFIG.equals(name)) {
            throw new IllegalArgumentException("'" + DEFAULT_CONFIG + "' can't be deleted.");
        }
        final ConfigEntry old = fileConfig.configs.remove(name);
        if (old != null) {
            write();
        }
    }

    @Override
    public List<ConfigEntry> listConfigs() {
        List<ConfigEntry> all = new ArrayList<>(fileConfig.configs.values());
        if (defaultConfig != null) {
            all.add(0, defaultConfig);
        }
        return all;
    }

    @Override
    public void setLastUsed(String name) throws IOException {
        fileConfig.last = name;
        write();
    }

    @Override
    public ConfigEntry getLastUsed() throws IOException {
        if (fileConfig.last != null) {
            return getConfig(fileConfig.last);
        }
        return null;
    }
}
