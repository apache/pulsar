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
package org.apache.pulsar.broker.authentication.oidc;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;

@CustomLog
class ConfigUtils {
    /**
     * Get configured property as a string. If not configured, return null.
     * @param conf - the configuration map
     * @param configProp - the property to get
     * @return a string from the conf or null, if the configuration property was not set
     */
    static String getConfigValueAsString(ServiceConfiguration conf,
                                                String configProp) throws IllegalArgumentException {
        String value = getConfigValueAsStringImpl(conf, configProp);
        log.info().attr("property", configProp).attr("value", value).log("Configuration");
        return value;
    }

    /**
     * Get configured property as a string. If not configured, return null.
     * @param conf - the configuration map
     * @param configProp - the property to get
     * @param defaultValue - the value to use if the configuration value is not set
     * @return a string from the conf or the default value
     */
    static String getConfigValueAsString(ServiceConfiguration conf, String configProp,
                                                String defaultValue) throws IllegalArgumentException {
        String value = getConfigValueAsStringImpl(conf, configProp);
        if (value == null) {
            value = defaultValue;
        }
        log.info().attr("property", configProp).attr("value", value).log("Configuration");
        return value;
    }

    /**
     * Get configured property as a set. Split using a comma delimiter and remove any extra whitespace surrounding
     * the commas. If not configured, return the empty set.
     *
     * @param conf - the map of configuration properties
     * @param configProp - the property (key) to get
     * @return a set of strings from the conf
     */
    static Set<String> getConfigValueAsSet(ServiceConfiguration conf, String configProp) {
        String value = getConfigValueAsStringImpl(conf, configProp);
        if (StringUtils.isBlank(value)) {
            log.info().attr("property", configProp).log("Configuration is the empty set");
            return Collections.emptySet();
        }
        Set<String> set = Arrays.stream(value.trim().split("\\s*,\\s*")).collect(Collectors.toSet());
        log.info()
                .attr("property", configProp)
                .attr("value", String.join(", ", set))
                .log("Configuration");
        return set;
    }

    private static String getConfigValueAsStringImpl(ServiceConfiguration conf,
                                                     String configProp) throws IllegalArgumentException {
        Object value = conf.getProperty(configProp);
        if (value instanceof String) {
            return (String) value;
        } else {
            return null;
        }
    }

    /**
     * Utility method to get an integer from the {@link ServiceConfiguration}. If the value is not a valid long or the
     * key is not present in the conf, the default value will be used.
     *
     * @param conf - the map of configuration properties
     * @param configProp - the property (key) to get
     * @param defaultValue - the value to use if the property is missing from the conf
     * @return a long
     */
    static int getConfigValueAsInt(ServiceConfiguration conf, String configProp, int defaultValue) {
        Object value = conf.getProperty(configProp);
        if (value instanceof Integer) {
            log.info().attr("property", configProp).attr("value", value).log("Configuration");
            return (Integer) value;
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException numberFormatException) {
                log.error()
                        .attr("property", configProp)
                        .attr("value", value)
                        .attr("defaultValue", defaultValue)
                        .exception(numberFormatException)
                        .log("Expected configuration to be a long, using default");
                return defaultValue;
            }
        } else {
            log.info()
                    .attr("property", configProp)
                    .attr("defaultValue", defaultValue)
                    .log("Configuration is using the default value");
            return defaultValue;
        }
    }

    /**
     * Utility method to get a boolean from the {@link ServiceConfiguration}. If the key is present in the conf,
     * return the default value. If key is present the value is not a valid boolean, the result will be false.
     *
     * @param conf - the map of configuration properties
     * @param configProp - the property (key) to get
     * @param defaultValue - the value to use if the property is missing from the conf
     * @return a boolean
     */
    static boolean getConfigValueAsBoolean(ServiceConfiguration conf, String configProp, boolean defaultValue) {
        Object value = conf.getProperty(configProp);
        if (value instanceof Boolean) {
            log.info().attr("property", configProp).attr("value", value).log("Configuration");
            return (boolean) value;
        } else if (value instanceof String) {
            boolean result = Boolean.parseBoolean((String) value);
            log.info().attr("property", configProp).attr("value", result).log("Configuration");
            return result;
        } else {
            log.info()
                    .attr("property", configProp)
                    .attr("defaultValue", defaultValue)
                    .log("Configuration is using the default value");
            return defaultValue;
        }
    }
}
