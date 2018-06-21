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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Utils for loading configuration data.
 */
public final class ConfigurationDataUtils {

    private ConfigurationDataUtils() {}

    public static <T> T loadData(Map<String, Object> config,
                                 T existingData,
                                 Class<T> dataCls) {
        ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
        try {
            String existingConfigJson = mapper.writeValueAsString(existingData);
            Map<String, Object> existingConfig = mapper.readValue(existingConfigJson, Map.class);
            Map<String, Object> newConfig = Maps.newHashMap();
            newConfig.putAll(existingConfig);
            newConfig.putAll(config);
            String configJson = mapper.writeValueAsString(newConfig);
            return mapper.readValue(configJson, dataCls);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config into existing configuration data");
        }

    }

}

