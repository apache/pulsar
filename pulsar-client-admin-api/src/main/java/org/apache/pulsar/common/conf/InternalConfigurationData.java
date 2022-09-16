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

    @Deprecated
    private String zookeeperServers;
    @Deprecated
    private String configurationStoreServers;
    private String metadataStoreUrl;
    private String configurationMetadataStoreUrl;
    @Deprecated
    private String ledgersRootPath;
    private String bookkeeperMetadataServiceUri;
    private String stateStorageServiceUrl;

    public InternalConfigurationData() {
    }

    public InternalConfigurationData(String zookeeperServers,
                                     String configurationMetadataStoreUrl,
                                     String ledgersRootPath,
                                     String bookkeeperMetadataServiceUri,
                                     String stateStorageServiceUrl) {
        this.metadataStoreUrl = zookeeperServers;
        this.zookeeperServers = zookeeperServers;
        this.configurationMetadataStoreUrl = configurationMetadataStoreUrl;
        this.configurationStoreServers = configurationMetadataStoreUrl;
        this.ledgersRootPath = ledgersRootPath;
        this.bookkeeperMetadataServiceUri = bookkeeperMetadataServiceUri;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
    }

    @Deprecated
    public String getZookeeperServers() {
        return zookeeperServers;
    }

    @Deprecated
    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    @Deprecated
    public String getConfigurationStoreServers() {
        return configurationStoreServers;
    }

    @Deprecated
    public void setConfigurationStoreServers(String configurationStoreServers) {
        this.configurationStoreServers = configurationStoreServers;
    }

    public String getMetadataStoreUrl() {
        if (metadataStoreUrl != null) {
            return metadataStoreUrl;
        } else if (zookeeperServers != null) {
            return "zk:" + zookeeperServers;
        }
        return null;
    }

    public String getConfigurationMetadataStoreUrl() {
        if (configurationMetadataStoreUrl != null) {
            return configurationMetadataStoreUrl;
        }
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
        return Objects.equals(metadataStoreUrl, other.metadataStoreUrl)
            && Objects.equals(configurationMetadataStoreUrl, other.configurationMetadataStoreUrl)
            && Objects.equals(ledgersRootPath, other.ledgersRootPath)
            && Objects.equals(bookkeeperMetadataServiceUri, other.bookkeeperMetadataServiceUri)
            && Objects.equals(stateStorageServiceUrl, other.stateStorageServiceUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataStoreUrl,
                configurationMetadataStoreUrl,
                ledgersRootPath,
                bookkeeperMetadataServiceUri,
                stateStorageServiceUrl);
    }

}
