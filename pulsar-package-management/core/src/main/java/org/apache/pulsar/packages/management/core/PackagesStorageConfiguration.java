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
package org.apache.pulsar.packages.management.core;

import java.util.Properties;

/**
 * Packages storage configuration is used to set and get the storage related configuration values.
 */
public interface PackagesStorageConfiguration {
    /**
     * Get a property with the key.
     *
     * @param key
     *          property key
     * @return the value
     */
    String getProperty(String key);

    /**
     * Set a property with the key.
     *
     * @param key
     *          property key
     * @param value
     *          property value
     */
    void setProperty(String key, String value);

    /**
     * Set a group of the property.
     *
     * @param properties
     *          a group of the property
     */
    void setProperty(Properties properties);
}
