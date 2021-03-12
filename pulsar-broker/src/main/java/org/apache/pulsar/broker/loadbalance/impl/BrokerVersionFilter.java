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
package org.apache.pulsar.broker.loadbalance.impl;

import com.github.zafarkhaja.semver.Version;
import java.util.Iterator;
import java.util.Set;
import org.apache.pulsar.broker.BrokerData;
import org.apache.pulsar.broker.BundleData;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.BrokerFilterBadVersionException;
import org.apache.pulsar.broker.loadbalance.LoadData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerVersionFilter implements BrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerVersionFilter.class);

    /**
     * Get the most recent broker version number from the load reports of all the running brokers. The version
     * number is from the build artifact in the pom and got added to the package when it was built by Maven
     *
     * @param brokers
     *            The brokers to choose the latest version string from.
     * @param loadData
     *            The load data from the leader broker (contains the load reports which
     *            in turn contain the version string).
     * @return The most recent broker version
     * @throws BrokerFilterBadVersionException
     *            If the most recent version is undefined (e.g., a bad broker version was encountered or a broker
     *            does not have a version string in its load report.
     */
    public Version getLatestVersionNumber(Set<String> brokers, LoadData loadData)
            throws BrokerFilterBadVersionException {
        if (null == brokers) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since broker set was null");
        }
        if (brokers.size() == 0) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since broker set was empty");
        }
        if (null == loadData) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since loadData was null");
        }

        Version latestVersion = null;
        for (String broker : brokers) {
            BrokerData data = loadData.getBrokerData().get(broker);
            if (null == data) {
                LOG.warn("No broker data for broker [{}]; disabling PreferLaterVersions feature", broker);
                // trigger the ModularLoadManager to reset all the brokers to the original set
                throw new BrokerFilterBadVersionException("No broker data for broker \"" + broker + "\"");
            }

            String brokerVersion = data.getLocalData().getBrokerVersionString();
            if (null == brokerVersion || brokerVersion.length() == 0) {
                LOG.warn("No version string in load report for broker [{}]; disabling PreferLaterVersions feature",
                        broker);
                // trigger the ModularLoadManager to reset all the brokers to the original set
                throw new BrokerFilterBadVersionException("No version string in load report for broker \""
                        + broker + "\"");
            }

            Version brokerVersionVersion = null;
            try {
                brokerVersionVersion = Version.valueOf(brokerVersion);
            } catch (Exception x) {
                LOG.warn("Invalid version string in load report for broker [{}]: [{}];"
                                + " disabling PreferLaterVersions feature",
                        broker, brokerVersion);
                // trigger the ModularLoadManager to reset all the brokers to the original set
                throw new BrokerFilterBadVersionException("Invalid version string in load report for broker \""
                        + broker + "\": \"" + brokerVersion + "\")");
            }

            if (null == latestVersion) {
                latestVersion = brokerVersionVersion;
            } else if (Version.BUILD_AWARE_ORDER.compare(latestVersion, brokerVersionVersion) < 0) {
                latestVersion = brokerVersionVersion;
            }
        }

        if (null == latestVersion) {
            throw new BrokerFilterBadVersionException("Unable to determine latest broker version");
        }

        return latestVersion;
    }

    /**
     * From the given set of available broker candidates, filter those using the version numbers.
     *
     * @param brokers
     *            The currently available brokers that have not already been filtered.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     */
    public void filter(Set<String> brokers, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf)
            throws BrokerFilterBadVersionException {

        if (!conf.isPreferLaterVersions()) {
            return;
        }

        com.github.zafarkhaja.semver.Version latestVersion = null;
        try {
            latestVersion = getLatestVersionNumber(brokers, loadData);
            LOG.info("Latest broker version found was [{}]", latestVersion);
        } catch (Exception x) {
            LOG.warn("Disabling PreferLaterVersions feature; reason: " + x.getMessage());
            throw new BrokerFilterBadVersionException("Cannot determine newest broker version: " + x.getMessage());
        }

        int numBrokersLatestVersion = 0;
        int numBrokersOlderVersion = 0;
        Iterator<String> brokerIterator = brokers.iterator();
        while (brokerIterator.hasNext()) {
            String broker = brokerIterator.next();
            BrokerData data = loadData.getBrokerData().get(broker);
            String brokerVersion = data.getLocalData().getBrokerVersionString();
            com.github.zafarkhaja.semver.Version brokerVersionVersion = Version.valueOf(brokerVersion);

            if (brokerVersionVersion.equals(latestVersion)) {
                LOG.debug("Broker [{}] is running the latest version ([{}])", broker, brokerVersion);
                ++numBrokersLatestVersion;
            } else {
                LOG.info("Broker [{}] is running an older version ([{}]); latest version is [{}]",
                        broker, brokerVersion, latestVersion);
                ++numBrokersOlderVersion;
                brokerIterator.remove();
            }
        }
        if (numBrokersOlderVersion == 0) {
            LOG.info("All {} brokers are running the latest version [{}]", numBrokersLatestVersion, latestVersion);
        }
    }
}
