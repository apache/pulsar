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

package org.apache.pulsar.bookie.rackawareness;

import java.util.ArrayList;
import java.util.List;

public class ConfigurationStringUtil {

    /**
     * The Bookkeeper configuration class converts comma delimited strings to ArrayLists, by default. Use
     * this method to ensure a configuration value is a {@link String}.
     * @param obj - object to convert to a string
     * @return The object's conversion to a string where Lists map to a comma delimited list.
     */
    static String castToString(Object obj) {
        if (null == obj) {
            return "";
        }
        if (obj instanceof List<?>) {
            List<String> result = new ArrayList<>();
            for (Object o : (List<?>) obj) {
                result.add((String) o);
            }
            return String.join(",", result);
        } else {
            return obj.toString();
        }
    }

}
