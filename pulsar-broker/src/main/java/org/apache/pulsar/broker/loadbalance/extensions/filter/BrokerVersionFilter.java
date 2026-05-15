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
package org.apache.pulsar.broker.loadbalance.extensions.filter;

import com.github.zafarkhaja.semver.Version;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterBadVersionException;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Filter by broker version.
 */
@CustomLog
public class BrokerVersionFilter implements BrokerFilter {

    public static final String FILTER_NAME = "broker_version_filter";


    /**
     * From the given set of available broker candidates, filter those old brokers using the version numbers.
     *
     * @param brokers The currently available brokers that have not already been filtered.
     * @param context The load manager context.
     *
     */
    @Override
    public CompletableFuture<Map<String, BrokerLookupData>> filterAsync(Map<String, BrokerLookupData> brokers,
                                                                        ServiceUnitId serviceUnit,
                                                                        LoadManagerContext context) {
        ServiceConfiguration conf = context.brokerConfiguration();
        if (!conf.isPreferLaterVersions() || brokers.isEmpty()) {
            return CompletableFuture.completedFuture(brokers);
        }

        Version latestVersion;
        try {
            latestVersion = getLatestVersionNumber(brokers);
            log.debug().attr("version", latestVersion).log("Latest broker version found");
        } catch (Exception ex) {
            log.warn().exceptionMessage(ex).log("Disabling PreferLaterVersions feature");
            return FutureUtil.failedFuture(
                    new BrokerFilterBadVersionException("Cannot determine newest broker version: " + ex.getMessage()));
        }

        int numBrokersLatestVersion = 0;
        int numBrokersOlderVersion = 0;

        Iterator<Map.Entry<String, BrokerLookupData>> brokerIterator = brokers.entrySet().iterator();
        while (brokerIterator.hasNext()) {
            Map.Entry<String, BrokerLookupData> next = brokerIterator.next();
            String brokerId = next.getKey();
            String version = next.getValue().brokerVersion();
            Version brokerVersionVersion = Version.valueOf(version);
            if (brokerVersionVersion.equals(latestVersion)) {
                log.debug().attr("broker", brokerId).attr("version", version)
                        .log("Broker is running the latest version");
                numBrokersLatestVersion++;
            } else {
                log.info().attr("broker", brokerId).attr("brokerVersion", version)
                        .attr("latestVersion", latestVersion)
                        .log("Broker is running an older version");
                numBrokersOlderVersion++;
                brokerIterator.remove();
            }
        }
        if (numBrokersOlderVersion == 0) {
            log.info().attr("brokerCount", numBrokersLatestVersion).attr("version", latestVersion)
                    .log("All brokers are running the latest version");
        }
        return CompletableFuture.completedFuture(brokers);
    }

    /**
     * Get the most recent broker version number from the broker lookup data of all the running brokers.
     * The version number is from the build artifact in the pom and got added to the package when it was built by Maven
     *
     * @param brokerMap
     *             The BrokerId -> BrokerLookupData Map.
     * @return The most recent broker version
     * @throws BrokerFilterBadVersionException
     *            If the most recent version is undefined (e.g., a bad broker version was encountered or a broker
     *            does not have a version string in its lookup data.
     */
    public Version getLatestVersionNumber(Map<String, BrokerLookupData> brokerMap)
            throws BrokerFilterBadVersionException {

        if (brokerMap.size() == 0) {
            throw new BrokerFilterBadVersionException(
                    "Unable to determine latest version since broker version map was empty");
        }

        Version latestVersion = null;
        for (Map.Entry<String, BrokerLookupData> entry : brokerMap.entrySet()) {
            String brokerId = entry.getKey();
            String version = entry.getValue().brokerVersion();
            if (null == version || version.length() == 0) {
                log.warn().attr("broker", brokerId)
                        .log("No version string in lookup data for broker; disabling PreferLaterVersions feature");
                // Trigger the load manager to reset all the brokers to the original set
                throw new BrokerFilterBadVersionException("No version string in lookup data for broker \""
                        + brokerId + "\"");
            }
            Version brokerVersionVersion;
            try {
                brokerVersionVersion = Version.valueOf(version);
            } catch (Exception x) {
                log.warn().attr("broker", brokerId).attr("version", version)
                        .log("Invalid version string in lookup data for broker;"
                                + " disabling PreferLaterVersions feature");
                // Trigger the load manager to reset all the brokers to the original set
                throw new BrokerFilterBadVersionException("Invalid version string in lookup data for broker \""
                        + brokerId + "\": \"" + version + "\")");
            }

            if (latestVersion == null) {
                latestVersion = brokerVersionVersion;
            } else if (Version.BUILD_AWARE_ORDER.compare(latestVersion, brokerVersionVersion) < 0) {
                latestVersion = brokerVersionVersion;
            }
        }

        return latestVersion;
    }

    @Override
    public String name() {
        return FILTER_NAME;
    }
}
