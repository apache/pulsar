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
package org.apache.pulsar.common.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal utilities for working with {@link Properties} objects.
 */
public class PulsarConfigurationUtils {

    private static final String BOOKKEEPER_CLIENT_CONFIG_PREFIX = "bookkeeper_";

    // This configuration must be applied to Distributed Log Configuration keys so that when DLog is initialized,
    // it applies the configuration to the bookkeeper client configuration.
    private static final String DLOG_BOOKKEEPER_SECTION_PREFIX = "bkc.";

    private static final Logger LOG = LoggerFactory.getLogger(PulsarConfigurationUtils.class);

    /**
     * Populates arbitrary configuration into the passed in {@link AbstractConfiguration} by filtering for the
     * {@link Properties} keys that have the prefix defined here {@link #BOOKKEEPER_CLIENT_CONFIG_PREFIX} and then
     * applying the key/value pairs after removing the prefix.
     * @param targetConf - the object to apply the bookkeeper client config
     * @param srcProps - properties in which to search for arbitrary bookkeeper client config
     * @param context - string to use when logging about applied configuration
     */
    public static void loadPrefixedBookieClientConfiguration(AbstractConfiguration targetConf,
                                                             Properties srcProps,
                                                             String context) {
        Map<String, Object> extraConfigs = getPrefixedProperties(BOOKKEEPER_CLIENT_CONFIG_PREFIX, srcProps, context);
        if (targetConf instanceof DistributedLogConfiguration) {
            extraConfigs.forEach((key, value) -> targetConf.setProperty(DLOG_BOOKKEEPER_SECTION_PREFIX + key, value));
        } else {
            extraConfigs.forEach(targetConf::setProperty);
        }
    }

    private static Map<String, Object> getPrefixedProperties(String prefix, Properties props, String context) {
        Map<String, Object> extraConfigs = new HashMap<>();
        int prefixLength = prefix.length();
        props.forEach((key, value) -> {
            String sKey = key.toString();
            if (sKey.startsWith(prefix) && value != null) {
                String truncatedKey = sKey.substring(prefixLength);
                LOG.info("Applying {} configuration {}, setting {}={}", context, sKey, truncatedKey, value);
                extraConfigs.put(truncatedKey, value);
            }
        });
        return extraConfigs;
    }
}
