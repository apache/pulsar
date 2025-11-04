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
package org.apache.pulsar.admin.cli;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.util.Properties;
import picocli.CommandLine.PropertiesDefaultProvider;

class PulsarAdminPropertiesProvider extends PropertiesDefaultProvider {
    private static final String webServiceUrlKey = "webServiceUrl";
    private final Properties properties;

    private PulsarAdminPropertiesProvider(Properties properties) {
        super(properties);
        this.properties = properties;
    }

    static PulsarAdminPropertiesProvider create(Properties properties) {
        Properties clone = (Properties) properties.clone();
        if (isBlank(properties.getProperty(webServiceUrlKey))) {
            String serviceUrl = properties.getProperty("serviceUrl");
            if (isNotBlank(serviceUrl)) {
                properties.put(webServiceUrlKey, serviceUrl);
            }
        }
        return new PulsarAdminPropertiesProvider(clone);
    }

    String getAdminUrl() {
        return properties.getProperty(webServiceUrlKey);
    }
}
