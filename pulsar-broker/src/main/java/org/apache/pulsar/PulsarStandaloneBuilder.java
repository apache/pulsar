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
package org.apache.pulsar;

import static org.apache.commons.lang3.StringUtils.isBlank;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;

public final class PulsarStandaloneBuilder {

    private PulsarStandalone pulsarStandalone;

    private PulsarStandaloneBuilder() {
        pulsarStandalone = new PulsarStandalone();
        pulsarStandalone.setWipeData(true);
        pulsarStandalone.setNoFunctionsWorker(true);
    }

    public static PulsarStandaloneBuilder instance() {
        return new PulsarStandaloneBuilder();
    }

    public PulsarStandaloneBuilder withConfig(ServiceConfiguration config) {
        pulsarStandalone.setConfig(config);
        return this;
    }

    public PulsarStandaloneBuilder withWipeData(boolean wipeData) {
        pulsarStandalone.setWipeData(wipeData);
        return this;
    }

    public PulsarStandaloneBuilder withNumOfBk(int numOfBk) {
        pulsarStandalone.setNumOfBk(numOfBk);
        return this;
    }

    public PulsarStandaloneBuilder withZkPort(int zkPort) {
        pulsarStandalone.setZkPort(zkPort);
        return this;
    }

    public PulsarStandaloneBuilder withBkPort(int bkPort) {
        pulsarStandalone.setBkPort(bkPort);
        return this;
    }

    public PulsarStandaloneBuilder withZkDir(String zkDir) {
        pulsarStandalone.setZkDir(zkDir);
        return this;
    }

    public PulsarStandaloneBuilder withBkDir(String bkDir) {
        pulsarStandalone.setBkDir(bkDir);
        return this;
    }

    public PulsarStandaloneBuilder withNoBroker(boolean noBroker) {
        pulsarStandalone.setNoBroker(noBroker);
        return this;
    }

    public PulsarStandaloneBuilder withOnlyBroker(boolean onlyBroker) {
        pulsarStandalone.setOnlyBroker(onlyBroker);
        return this;
    }

    public PulsarStandaloneBuilder withNoStreamStorage(boolean noStreamStorage) {
        pulsarStandalone.setNoStreamStorage(noStreamStorage);
        return this;
    }

    public PulsarStandaloneBuilder withStreamStoragePort(int streamStoragePort) {
        pulsarStandalone.setStreamStoragePort(streamStoragePort);
        return this;
    }

    public PulsarStandaloneBuilder withAdvertisedAddress(String advertisedAddress) {
        pulsarStandalone.setAdvertisedAddress(advertisedAddress);
        return this;
    }

    public PulsarStandalone build() {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("standalone");
        pulsarStandalone.setConfig(config);
        String zkServers = "127.0.0.1";

        if (pulsarStandalone.getAdvertisedAddress() != null) {
            // Use advertised address from command line
            pulsarStandalone.getConfig().setAdvertisedAddress(pulsarStandalone.getAdvertisedAddress());
            zkServers = pulsarStandalone.getAdvertisedAddress();
        } else if (isBlank(pulsarStandalone.getConfig().getAdvertisedAddress())) {
            // Use advertised address as local hostname
            pulsarStandalone.getConfig().setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
        } else {
            // Use advertised address from config file
        }

        // Set ZK server's host to localhost
        pulsarStandalone.getConfig().setZookeeperServers(zkServers + ":" + pulsarStandalone.getZkPort());
        pulsarStandalone.getConfig().setConfigurationStoreServers(zkServers + ":" + pulsarStandalone.getZkPort());
        pulsarStandalone.getConfig().setRunningStandalone(true);
        return pulsarStandalone;
    }

}
