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
package org.apache.pulsar.client.impl.auth.oauth2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Get configured property as a string. If not configured, return null.
     * @param params - the parameters
     * @param configProp - the property to get
     * @return a string from the conf or null, if the configuration property was not set
     */
    static String getConfigValueAsString(Map<String, String> params,
                                         String configProp) throws IllegalArgumentException {
        String value = params.get(configProp);
        log.info("Configuration for [{}] is [{}]", configProp, value);
        return value;
    }

    /**
     * Get configured property as a string. If not configured, return the default value.
     * @param params - the parameters
     * @param configProp - the property to get
     * @param defaultValue - the value to use if the configuration value is not set
     * @return a string from the conf or the default value
     */
    static String getConfigValueAsString(Map<String, String> params, String configProp,
                                                String defaultValue) throws IllegalArgumentException {
        String value = params.get(configProp);
        if (value == null) {
            value = defaultValue;
        }
        log.info("Configuration for [{}] is [{}]", configProp, value);
        return value;
    }

    /**
     * Get configured property as a set. Split using a comma delimiter and remove any extra whitespace surrounding
     * the commas. If not configured, return the empty set.
     *
     * @param params - the map of configuration properties
     * @param configProp - the property (key) to get
     * @return a set of strings from the conf
     */
    static Set<String> getConfigValueAsSet(Map<String, String> params, String configProp) {
        String value = params.get(configProp);
        if (StringUtils.isBlank(value)) {
            log.info("Configuration for [{}] is the empty set.", configProp);
            return Collections.emptySet();
        }
        Set<String> set = Arrays.stream(value.trim().split("\\s*,\\s*")).collect(Collectors.toSet());
        log.info("Configuration for [{}] is [{}].", configProp, String.join(", ", set));
        return set;
    }

    /**
     * Get configured property as an integer. If the value is not a valid long or the
     * key is not present in the conf, the default value will be used.
     *
     * @param params - the parameters
     * @param configProp - the property (key) to get
     * @param defaultValue - the value to use if the property is missing from the conf
     * @return a long
     */
    static int getConfigValueAsInt(Map<String, String> params, String configProp, int defaultValue) {
        String value = params.get(configProp);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException numberFormatException) {
                log.error("Expected configuration for [{}] to be an integer, but got [{}]. Using default value: [{}]",
                        configProp, value, defaultValue, numberFormatException);
                return defaultValue;
            }
        } else {
            log.info("Configuration for [{}] is using the default value: [{}]", configProp, defaultValue);
            return defaultValue;
        }
    }

    /**
     * Get configured property as an boolean. If the key is not present in the conf,
     * return the default value. If key is present the value is not a valid boolean, the result will be false.
     *
     * @param params - the map of configuration properties
     * @param configProp - the property (key) to get
     * @param defaultValue - the value to use if the property is missing from the conf
     * @return a boolean
     */
    static boolean getConfigValueAsBoolean(Map<String, String> params, String configProp, boolean defaultValue) {
        String value = params.get(configProp);
        if (StringUtils.isNotBlank(value)) {
            boolean result = Boolean.parseBoolean((String) value);
            log.info("Configuration for [{}] is [{}]", configProp, result);
            return result;
        } else {
            log.info("Configuration for [{}] is using the default value: [{}]", configProp, defaultValue);
            return defaultValue;
        }
    }

    static File getConfigValueAsFile(Map<String, String> params, String configProp) {
        String value = params.get(configProp);
        if (StringUtils.isNotBlank(value)) {
            return new File(value);
        }
        return null;
    }

}
