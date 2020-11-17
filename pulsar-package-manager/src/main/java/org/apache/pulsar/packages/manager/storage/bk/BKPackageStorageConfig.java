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

package org.apache.pulsar.packages.manager.storage.bk;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.packages.manager.PackageStorageConfig;

/**
 * BookKeeper storage config args.
 */
@Data
@Setter
@Getter
@Builder
class BKPackageStorageConfig {
    int numReplicas;
    String zkServers;
    String ledgersRootPath;
    String bookkeeperClientAuthenticationPlugin;
    String bookkeeperClientAuthenticationParametersName;
    String bookkeeperClientAuthenticationParameters;

    public static BKPackageStorageConfig loadFromPackageConfiguration(PackageStorageConfig config) {
        return BKPackageStorageConfig.builder()
            .numReplicas(config.getNumReplicas())
            .zkServers(config.getZkServers())
            .ledgersRootPath(config.getLedgersRootPath())
            .bookkeeperClientAuthenticationPlugin(config.getBookkeeperClientAuthenticationPlugin())
            .bookkeeperClientAuthenticationParametersName(config.getBookkeeperClientAuthenticationParametersName())
            .bookkeeperClientAuthenticationParameters(config.getBookkeeperClientAuthenticationParameters()).build();
    }
}
