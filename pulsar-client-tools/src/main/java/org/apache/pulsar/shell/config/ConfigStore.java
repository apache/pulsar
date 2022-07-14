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
package org.apache.pulsar.shell.config;

import java.io.IOException;
import java.util.List;
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
}
