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

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    /**
     * Get configured property as an integer. If the value is not a valid integer or the
     * key is not present in the conf, the default value will be used.
     *
     * @param params       - the parameters
     * @param configProp   - the property (key) to get
     * @param defaultValue - the value to use if the property is missing from the conf
     * @return an integer
     */
    static int getConfigValueAsInt(Map<String, String> params, String configProp, int defaultValue) {
        String value = params.get(configProp);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Integer.parseInt(value);
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

}
