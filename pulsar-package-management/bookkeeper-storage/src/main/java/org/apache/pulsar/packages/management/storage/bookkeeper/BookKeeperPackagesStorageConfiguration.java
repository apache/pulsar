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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import java.util.Properties;
import org.apache.pulsar.packages.management.core.PackagesStorageConfiguration;
import org.apache.pulsar.packages.management.core.impl.DefaultPackagesStorageConfiguration;

public class BookKeeperPackagesStorageConfiguration implements PackagesStorageConfiguration {

    private final PackagesStorageConfiguration configuration;

    BookKeeperPackagesStorageConfiguration() {
        this.configuration = new DefaultPackagesStorageConfiguration();
    }

    BookKeeperPackagesStorageConfiguration(PackagesStorageConfiguration configuration) {
        this.configuration = configuration;
    }

    int getPackagesReplicas() {
        return Integer.parseInt(getProperty("packagesReplicas"));
    }

    String getZookeeperServers() {
        return getProperty("zookeeperServers");
    }

    String getPackagesManagementLedgerRootPath() {
        return getProperty("packagesManagementLedgerRootPath");
    }

    String getBookkeeperClientAuthenticationPlugin() {
        return getProperty("bookkeeperClientAuthenticationPlugin");
    }

    String getBookkeeperClientAuthenticationParametersName() {
        return getProperty("bookkeeperClientAuthenticationParametersName");
    }

    String getBookkeeperClientAuthenticationParameters() {
        return getProperty("bookkeeperClientAuthenticationParameters");
    }


    @Override
    public String getProperty(String key) {
        return configuration.getProperty(key);
    }

    @Override
    public void setProperty(String key, String value) {
        configuration.setProperty(key, value);
    }

    @Override
    public void setProperty(Properties properties) {
        this.configuration.setProperty(properties);
    }
}
