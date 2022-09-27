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
package org.apache.pulsar.client.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Internal utility methods for filtering and mapping {@link Properties} objects.
 */
public class PropertiesUtils {

    /**
     * Filters the {@link Properties} object so that only properties with the configured prefix are retained,
     * and then removes that prefix and puts the key value pairs into the result map.
     * @param props - the properties object to filter
     * @param prefix - the prefix to filter against and then remove for keys in the resulting map
     * @return a map of properties
     */
    public static Map<String, Object> filterAndMapProperties(Properties props, String prefix) {
        return filterAndMapProperties(props, prefix, "");
    }

    /**
     * Filters the {@link Properties} object so that only properties with the configured prefix are retained,
     * and then replaces the srcPrefix with the targetPrefix when putting the key value pairs in the resulting map.
     * @param props - the properties object to filter
     * @param srcPrefix - the prefix to filter against and then remove for keys in the resulting map
     * @param targetPrefix - the prefix to add to keys in the result map
     * @return a map of properties
     */
    public static Map<String, Object> filterAndMapProperties(Properties props, String srcPrefix, String targetPrefix) {
        Map<String, Object> result = new HashMap<>();
        int prefixLength = srcPrefix.length();
        props.forEach((keyObject, value) -> {
            if (!(keyObject instanceof String)) {
                return;
            }
            String key = (String) keyObject;
            if (key.startsWith(srcPrefix) && value != null) {
                String truncatedKey = key.substring(prefixLength);
                result.put(targetPrefix + truncatedKey, value);
            }
        });
        return result;
    }
}
