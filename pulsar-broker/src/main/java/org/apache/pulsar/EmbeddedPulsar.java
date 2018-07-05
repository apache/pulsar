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

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class EmbeddedPulsar extends StandalonePulsarBase {

    private EmbeddedPulsar(){}

    public static EmbeddedPulsarBuilder builder(){
        return EmbeddedPulsarBuilder.instance();
    }

    public static void main(String[] args) throws Exception {
        EmbeddedPulsar embeddedPulsar = EmbeddedPulsarBuilder.instance().build();
        embeddedPulsar.start();
        Thread.sleep(10000);
        embeddedPulsar.stop();
        System.exit(0);
    }

    public static final class EmbeddedPulsarBuilder {
        private EmbeddedPulsar embeddedPulsar;

        private EmbeddedPulsarBuilder() {
            embeddedPulsar = new EmbeddedPulsar();
            embeddedPulsar.setWipeData(true);
            embeddedPulsar.setNoFunctionsWorker(true);
        }

        public static EmbeddedPulsarBuilder instance() {
            return new EmbeddedPulsarBuilder();
        }

        public EmbeddedPulsarBuilder withConfig(ServiceConfiguration config) {
            embeddedPulsar.setConfig(config);
            return this;
        }

        public EmbeddedPulsarBuilder withWipeData(boolean wipeData) {
            embeddedPulsar.setWipeData(wipeData);
            return this;
        }

        public EmbeddedPulsarBuilder withNumOfBk(int numOfBk) {
            embeddedPulsar.setNumOfBk(numOfBk);
            return this;
        }

        public EmbeddedPulsarBuilder withZkPort(int zkPort) {
            embeddedPulsar.setZkPort(zkPort);
            return this;
        }

        public EmbeddedPulsarBuilder withBkPort(int bkPort) {
            embeddedPulsar.setBkPort(bkPort);
            return this;
        }

        public EmbeddedPulsarBuilder withZkDir(String zkDir) {
            embeddedPulsar.setZkDir(zkDir);
            return this;
        }

        public EmbeddedPulsarBuilder withBkDir(String bkDir) {
            embeddedPulsar.setBkDir(bkDir);
            return this;
        }

        public EmbeddedPulsarBuilder withNoBroker(boolean noBroker) {
            embeddedPulsar.setNoBroker(noBroker);
            return this;
        }

        public EmbeddedPulsarBuilder withOnlyBroker(boolean onlyBroker) {
            embeddedPulsar.setOnlyBroker(onlyBroker);
            return this;
        }

        public EmbeddedPulsarBuilder withNoStreamStorage(boolean noStreamStorage) {
            embeddedPulsar.setNoStreamStorage(noStreamStorage);
            return this;
        }

        public EmbeddedPulsarBuilder withStreamStoragePort(int streamStoragePort) {
            embeddedPulsar.setStreamStoragePort(streamStoragePort);
            return this;
        }

        public EmbeddedPulsarBuilder withAdvertisedAddress(String advertisedAddress) {
            embeddedPulsar.setAdvertisedAddress(advertisedAddress);
            return this;
        }

        public EmbeddedPulsar build() {
            ServiceConfiguration config = new ServiceConfiguration();
            config.setClusterName("standalone");
            embeddedPulsar.setConfig(config);
            String zkServers = "127.0.0.1";

            if (embeddedPulsar.getAdvertisedAddress() != null) {
                // Use advertised address from command line
                embeddedPulsar.getConfig().setAdvertisedAddress(embeddedPulsar.getAdvertisedAddress());
                zkServers = embeddedPulsar.getAdvertisedAddress();
            } else if (isBlank(embeddedPulsar.getConfig().getAdvertisedAddress())) {
                // Use advertised address as local hostname
                embeddedPulsar.getConfig().setAdvertisedAddress(ServiceConfigurationUtils.unsafeLocalhostResolve());
            } else {
                // Use advertised address from config file
            }

            // Set ZK server's host to localhost
            embeddedPulsar.getConfig().setZookeeperServers(zkServers + ":" + embeddedPulsar.getZkPort());
            embeddedPulsar.getConfig().setConfigurationStoreServers(zkServers + ":" + embeddedPulsar.getZkPort());
            embeddedPulsar.getConfig().setRunningStandalone(true);
            return embeddedPulsar;
        }
    }
}
