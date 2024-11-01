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
package org.apache.pulsar.tests.integration.utils;

import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Slf4j
public class IntegTestUtils {
    private static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    protected static String generateNamespaceName() {
        return "ns-" + randomName(8);
    }

    protected static String generateTopicName(String topicPrefix, boolean isPersistent) {
        return generateTopicName("default", topicPrefix, isPersistent);
    }

    protected static String generateTopicName(String namespace, String topicPrefix, boolean isPersistent) {
        String topicName = new StringBuilder(topicPrefix)
                .append("-")
                .append(randomName(8))
                .append("-")
                .append(System.currentTimeMillis())
                .toString();
        if (isPersistent) {
            return "persistent://public/" + namespace + "/" + topicName;
        } else {
            return "non-persistent://public/" + namespace + "/" + topicName;
        }
    }

    public static String getNonPartitionedTopic(PulsarAdmin admin, String topicPrefix, boolean isPersistent)
            throws Exception {
        String nsName = generateNamespaceName();
        admin.namespaces().createNamespace("public/" + nsName);
        return generateTopicName(nsName, topicPrefix, isPersistent);
    }

    public static String getPartitionedTopic(PulsarAdmin admin, String topicPrefix, boolean isPersistent,
                                             int partitions) throws Exception {
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must greater than 1");
        }
        String nsName = generateNamespaceName();
        admin.namespaces().createNamespace("public/" + nsName);
        String topicName = generateTopicName(nsName, topicPrefix, isPersistent);
        admin.topics().createPartitionedTopic(topicName, partitions);
        return topicName;
    }
}
