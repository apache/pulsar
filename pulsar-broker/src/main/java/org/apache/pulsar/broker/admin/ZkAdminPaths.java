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
package org.apache.pulsar.broker.admin;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

public class ZkAdminPaths {

    public static final String POLICIES = "policies";
    public static final String PARTITIONED_TOPIC_PATH_ZNODE = "partitioned-topics";

    public static String partitionedTopicPath(TopicName name) {
        return adminPath(PARTITIONED_TOPIC_PATH_ZNODE,
                name.getNamespace(), name.getDomain().value(), name.getEncodedLocalName());
    }

    public static String managedLedgerPath(TopicName name) {
        return "/managed-ledgers/" + name.getPersistenceNamingEncoding();
    }

    public static String namespacePoliciesPath(NamespaceName name) {
        return adminPath(POLICIES, name.toString());
    }

    private static String adminPath(String... parts) {
        return "/admin/" + String.join("/", parts);
    }

    private ZkAdminPaths() {}
}
