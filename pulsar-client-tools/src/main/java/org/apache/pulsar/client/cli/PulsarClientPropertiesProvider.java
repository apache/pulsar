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
package org.apache.pulsar.client.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.util.Properties;
import picocli.CommandLine.PropertiesDefaultProvider;

class PulsarClientPropertiesProvider extends PropertiesDefaultProvider {
    private static final String brokerServiceUrlKey = "brokerServiceUrl";
    private final Properties properties;

    private PulsarClientPropertiesProvider(Properties properties) {
        super(properties);
        this.properties = properties;
    }

    static PulsarClientPropertiesProvider create(Properties properties) {
        Properties clone = (Properties) properties.clone();
        String brokerServiceUrl = clone.getProperty(brokerServiceUrlKey);
        if (isBlank(brokerServiceUrl)) {
            String serviceUrl = clone.getProperty("webServiceUrl");
            if (isBlank(serviceUrl)) {
                // fallback to previous-version serviceUrl property to maintain backward-compatibility
                serviceUrl = clone.getProperty("serviceUrl");
            }
            if (isNotBlank(serviceUrl)) {
                clone.put(brokerServiceUrlKey, serviceUrl);
            }
        }
        return new PulsarClientPropertiesProvider(clone);
    }

    String getServiceUrl() {
        return properties.getProperty(brokerServiceUrlKey);
    }
}
