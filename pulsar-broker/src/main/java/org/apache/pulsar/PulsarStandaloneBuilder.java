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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

public final class PulsarStandaloneBuilder {

    private static final Logger log = LoggerFactory.getLogger(PulsarStandaloneBuilder.class);

    private final PulsarStandalone pulsarStandalone;

    private PulsarStandaloneBuilder() {
        pulsarStandalone = new PulsarStandalone();
        pulsarStandalone.setWipeData(true);
        pulsarStandalone.setNoFunctionsWorker(true);
    }

    public static PulsarStandaloneBuilder instance() {
        return new PulsarStandaloneBuilder();
    }

    public PulsarStandaloneBuilder withConfigFile(String configFile) {
        pulsarStandalone.setConfigFile(configFile);
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
        // Don't break existing code which does not have the config file set
        if (pulsarStandalone.getConfigFile() != null) {
            // Change IOException and ConfigurationException into a RuntimeException, because if the
            // config file isn't readable, there is nothing a caller can do, so don't bother with
            // a checked exception that needs to be catched
            try {
                // By reading the configuration file here, the user can modify the configurations before
                // calling PulsarStandalone.start()
                ServerConfiguration bkServerConf = new ServerConfiguration();
                bkServerConf.loadConf(new File(pulsarStandalone.getConfigFile()).toURI().toURL());
                pulsarStandalone.setBkServerConfig(bkServerConf);

                pulsarStandalone.setConfig(PulsarConfigurationLoader.create(
                        new FileInputStream(pulsarStandalone.getConfigFile()), ServiceConfiguration.class));
            } catch (IOException | ConfigurationException e) {
                // IllegalArgumentException seems appropriate here as the config file was used as an "argument"
                // by using #withConfigFile
                throw new IllegalArgumentException("Config file could not be read: " + pulsarStandalone.getConfigFile(), e);
            }
        } else {
            pulsarStandalone.setConfig(new ServiceConfiguration());
        }

        if (pulsarStandalone.getConfig().getClusterName() == null) {
            pulsarStandalone.getConfig().setClusterName("standalone");
        }

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
