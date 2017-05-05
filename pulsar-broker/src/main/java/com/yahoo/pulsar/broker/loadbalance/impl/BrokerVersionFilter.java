/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.loadbalance.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.loadbalance.BrokerFilter;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.loadbalance.LoadData;

public class BrokerVersionFilter implements BrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(GenericBrokerHostUsageImpl.class);

    /**
     * From the given set of available broker candidates, filter those using the load data.
     *
     * @param brokers
     *            The currently available brokers that have not already been filtered. This set may be modified by
     *            filter.
     * @param bundleToAssign
     *            The data for the bundle to assign.
     * @param loadData
     *            The load data from the leader broker.
     * @param conf
     *            The service configuration.
     */
    public void filter(Set<String> brokers, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf) {
        if ( conf.getActiveVersion() == null || conf.getActiveVersion().length() == 0 ) {
            return;
        }

        final String activeVersion = conf.getActiveVersion();

        Map<String, BrokerData> brokerData = loadData.getBrokerData();
        List<String> brokersToRemove = new LinkedList<>();
        for ( String broker : brokers ) {
            BrokerData data = brokerData.get(broker);
            if ( null == data ) {
                LOG.warn("Cannot determine version of broker [{}]", broker);
                continue;
            }

            String brokerVersion = data.getLocalData().getBrokerVersionString();
            if ( null == brokerVersion || brokerVersion.length() == 0 ) {
                LOG.warn("Broker [{}] load report does not contain a version string", broker);
                continue;
            }

            LOG.debug("Broker [{}] is running version [{}]", broker, brokerVersion);
            if ( !brokerVersion.equalsIgnoreCase(activeVersion)) {
                LOG.info("Not considering broker {} since it is on a non-active version {} (active version is {}",
                        broker, brokerVersion, conf.getActiveVersion());
                brokersToRemove.add(broker);
            }
        }

        if ( brokersToRemove.size() == brokers.size() ) {
            LOG.warn("No broker is running with the active version ([{}]), so choosing among all candidate brokers",
                    conf.getActiveVersion());
        } else if ( brokersToRemove.isEmpty() ) {
            LOG.debug("All brokers are running on the active version ([{}])", conf.getActiveVersion());
        } else {
            int numBrokersOnInactiveVersion = brokersToRemove.size();
            int numBrokersOnActiveVersion = brokers.size() - numBrokersOnInactiveVersion;
            LOG.info("Multiple broker versions encountered: {} are on the active version [{}] and {} are on an inactive version",
                    numBrokersOnActiveVersion, conf.getActiveVersion(), numBrokersOnInactiveVersion);
            brokers.removeAll(brokersToRemove);
        }
    }
}
