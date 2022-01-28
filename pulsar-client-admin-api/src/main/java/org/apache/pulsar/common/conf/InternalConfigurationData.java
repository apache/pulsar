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
package org.apache.pulsar.common.conf;

import java.util.Objects;
import lombok.ToString;

/**
 * Internal configuration data.
 */
@ToString
public class InternalConfigurationData {

    private String zookeeperServers;
    private String configurationStoreServers;
    @Deprecated
    private String ledgersRootPath;
    private String bookkeeperMetadataServiceUri;
    private String stateStorageServiceUrl;

    public InternalConfigurationData() {
    }

    public InternalConfigurationData(String zookeeperServers,
                                     String configurationStoreServers,
                                     String ledgersRootPath,
                                     String bookkeeperMetadataServiceUri,
                                     String stateStorageServiceUrl) {
        this.zookeeperServers = zookeeperServers;
        this.configurationStoreServers = configurationStoreServers;
        this.ledgersRootPath = ledgersRootPath;
        this.bookkeeperMetadataServiceUri = bookkeeperMetadataServiceUri;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public String getConfigurationStoreServers() {
        return configurationStoreServers;
    }

    /** @deprecated */
    @Deprecated
    public String getLedgersRootPath() {
        return ledgersRootPath;
    }

    public String getBookkeeperMetadataServiceUri() {
        return bookkeeperMetadataServiceUri;
    }

    public String getStateStorageServiceUrl() {
        return stateStorageServiceUrl;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InternalConfigurationData)) {
            return false;
        }
        InternalConfigurationData other = (InternalConfigurationData) obj;
        return Objects.equals(zookeeperServers, other.zookeeperServers)
            && Objects.equals(configurationStoreServers, other.configurationStoreServers)
            && Objects.equals(ledgersRootPath, other.ledgersRootPath)
            && Objects.equals(bookkeeperMetadataServiceUri, other.bookkeeperMetadataServiceUri)
            && Objects.equals(stateStorageServiceUrl, other.stateStorageServiceUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(zookeeperServers,
                configurationStoreServers,
                ledgersRootPath,
                bookkeeperMetadataServiceUri,
                stateStorageServiceUrl);
    }

}
