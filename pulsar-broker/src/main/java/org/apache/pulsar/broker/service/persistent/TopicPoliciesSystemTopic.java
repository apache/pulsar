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
package org.apache.pulsar.broker.service.persistent;


import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;

public class TopicPoliciesSystemTopic extends SystemTopic {
    public static final String IS_GLOBAL = "isGlobal";


    public TopicPoliciesSystemTopic(String topic, ManagedLedger ledger,
                                    BrokerService brokerService)
            throws BrokerServiceException.NamingException, PulsarServerException {
        super(topic, ledger, brokerService);
    }

    @Override
    protected boolean addReplicationCluster(String remoteCluster, ManagedCursor cursor, String localCluster) {
        boolean isReplicatorStarted = super.addReplicationCluster(remoteCluster, cursor, localCluster);
        if (isReplicatorStarted) {
            getReplicators().get(remoteCluster).setFilterFunction((messageImpl)
                    -> !IS_GLOBAL.equals(messageImpl.getProperty(IS_GLOBAL)));
        }
        return isReplicatorStarted;
    }


}
