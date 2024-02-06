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
package org.apache.pulsar.common.naming;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.Codec;

/**
 * Encapsulate the parsing of the completeTopicName name.
 */
public class TopicName implements ServiceUnitId {

    public static final String PUBLIC_TENANT = "public";
    public static final String DEFAULT_NAMESPACE = "default";

    public static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private final String completeTopicName;

    private final TopicDomain domain;
    private final String tenant;
    private final String cluster;
    private final String namespacePortion;
    private final String localName;

    private final NamespaceName namespaceName;

    private final int partitionIndex;

    private static final LoadingCache<String, TopicName> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, TopicName>() {
                @Override
                public TopicName load(String name) throws Exception {
                    return new TopicName(name);
                }
            });

    public static final TopicName TRANSACTION_COORDINATOR_ASSIGN = TopicName.get(TopicDomain.persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE, "transaction_coordinator_assign");

    public static final TopicName TRANSACTION_COORDINATOR_LOG = TopicName.get(TopicDomain.persistent.value(),
            NamespaceName.SYSTEM_NAMESPACE, "__transaction_log_");

    public static final String PENDING_ACK_STORE_SUFFIX = "__transaction_pending_ack";
    public static TopicName get(String domain, NamespaceName namespaceName, String topic) {
        String name = domain + "://" + namespaceName.toString() + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String tenant, String namespace, String topic) {
        String name = domain + "://" + tenant + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String tenant, String cluster, String namespace,
                                String topic) {
        String name = domain + "://" + tenant + '/' + cluster + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String topic) {
        try {
            return cache.get(topic);
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    public static TopicName getPartitionedTopicName(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (topicName.isPartitioned()) {
            return TopicName.get(topicName.getPartitionedTopicName());
        }
        return topicName;
    }

    public static boolean isValid(String topic) {
        try {
            get(topic);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private TopicName(String completeTopicName) {
        try {
            // The topic name can be in two different forms, one is fully qualified topic name,
            // the other one is short topic name
            if (!completeTopicName.contains("://")) {
                // The short topic name can be:
                // - <topic>
                // - <property>/<namespace>/<topic>
                String[] parts = StringUtils.split(completeTopicName, '/');
                if (parts.length == 3) {
                    completeTopicName = TopicDomain.persistent.name() + "://" + completeTopicName;
                } else if (parts.length == 1) {
                    completeTopicName = TopicDomain.persistent.name() + "://"
                        + PUBLIC_TENANT + "/" + DEFAULT_NAMESPACE + "/" + parts[0];
                } else {
                    throw new IllegalArgumentException(
                        "Invalid short topic name '" + completeTopicName + "', it should be in the format of "
                        + "<tenant>/<namespace>/<topic> or <topic>");
                }
            }

            // The fully qualified topic name can be in two different forms:
            // new:    persistent://tenant/namespace/topic
            // legacy: persistent://tenant/cluster/namespace/topic

            List<String> parts = Splitter.on("://").limit(2).splitToList(completeTopicName);
            this.domain = TopicDomain.getEnum(parts.get(0));

            String rest = parts.get(1);

            // The rest of the name can be in different forms:
            // new:    tenant/namespace/<localName>
            // legacy: tenant/cluster/namespace/<localName>
            // Examples of localName:
            // 1. some, name, xyz
            // 2. xyz-123, feeder-2


            parts = Splitter.on("/").limit(4).splitToList(rest);
            if (parts.size() == 3) {
                // New topic name without cluster name
                this.tenant = parts.get(0);
                this.cluster = null;
                this.namespacePortion = parts.get(1);
                this.localName = parts.get(2);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, namespacePortion);
            } else if (parts.size() == 4) {
                // Legacy topic name that includes cluster name
                this.tenant = parts.get(0);
                this.cluster = parts.get(1);
                this.namespacePortion = parts.get(2);
                this.localName = parts.get(3);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(tenant, cluster, namespacePortion);
            } else {
                throw new IllegalArgumentException("Invalid topic name: " + completeTopicName);
            }


            if (localName == null || localName.isEmpty()) {
                throw new IllegalArgumentException("Invalid topic name: " + completeTopicName);
            }

        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid topic name: " + completeTopicName, e);
        }
        if (isV2()) {
            this.completeTopicName = String.format("%s://%s/%s/%s",
                                                   domain, tenant, namespacePortion, localName);
        } else {
            this.completeTopicName = String.format("%s://%s/%s/%s/%s",
                                                   domain, tenant, cluster,
                                                   namespacePortion, localName);
        }
    }

    public boolean isPersistent() {
        return TopicDomain.persistent == domain;
    }

    /**
     * Extract the namespace portion out of a completeTopicName name.
     *
     * <p>Works both with old & new convention.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespaceName.toString();
    }

    /**
     * Get the namespace object that this completeTopicName belongs to.
     *
     * @return namespace object
     */
    @Override
    public NamespaceName getNamespaceObject() {
        return namespaceName;
    }

    public TopicDomain getDomain() {
        return domain;
    }

    public String getTenant() {
        return tenant;
    }

    @Deprecated
    public String getCluster() {
        return cluster;
    }

    public String getNamespacePortion() {
        return namespacePortion;
    }

    public String getLocalName() {
        return localName;
    }

    public String getEncodedLocalName() {
        return Codec.encode(localName);
    }

    public TopicName getPartition(int index) {
        if (index == -1 || this.toString().contains(PARTITIONED_TOPIC_SUFFIX)) {
            return this;
        }
        String partitionName = this.toString() + PARTITIONED_TOPIC_SUFFIX + index;
        return get(partitionName);
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public int getPartitionIndex() {
        return partitionIndex;
    }

    public boolean isPartitioned() {
        return partitionIndex != -1;
    }

    /**
     * For partitions in a topic, return the base partitioned topic name.
     * Eg:
     * <ul>
     *  <li><code>persistent://prop/cluster/ns/my-topic-partition-1</code> -->
     *  <code>persistent://prop/cluster/ns/my-topic</code>
     *  <li><code>persistent://prop/cluster/ns/my-topic</code> --> <code>persistent://prop/cluster/ns/my-topic</code>
     * </ul>
     */
    public String getPartitionedTopicName() {
        if (isPartitioned()) {
            return completeTopicName.substring(0, completeTopicName.lastIndexOf("-partition-"));
        } else {
            return completeTopicName;
        }
    }

    /**
     * @return partition index of the completeTopicName.
     * It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public static int getPartitionIndex(String topic) {
        int partitionIndex = -1;
        if (topic.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                String idx = StringUtils.substringAfterLast(topic, PARTITIONED_TOPIC_SUFFIX);
                partitionIndex = Integer.parseInt(idx);
                if (partitionIndex < 0) {
                    // for the "topic-partition--1"
                    partitionIndex = -1;
                } else if (StringUtils.length(idx) != String.valueOf(partitionIndex).length()) {
                    // for the "topic-partition-01"
                    partitionIndex = -1;
                }
            } catch (NumberFormatException nfe) {
                // ignore exception
            }
        }

        return partitionIndex;
    }

    /**
     * Returns the http rest path for use in the admin web service.
     * Eg:
     *   * "persistent/my-tenant/my-namespace/my-topic"
     *   * "non-persistent/my-tenant/my-namespace/my-topic"
     *
     * @return topic rest path
     */
    public String getRestPath() {
        return getRestPath(true);
    }

    public String getRestPath(boolean includeDomain) {
        String domainName = includeDomain ? domain + "/" : "";
        if (isV2()) {
            return String.format("%s%s/%s/%s", domainName, tenant, namespacePortion, getEncodedLocalName());
        } else {
            return String.format("%s%s/%s/%s/%s", domainName, tenant, cluster, namespacePortion, getEncodedLocalName());
        }
    }

    /**
     * Returns the name of the persistence resource associated with the completeTopicName.
     *
     * @return the relative path to be used in persistence
     */
    public String getPersistenceNamingEncoding() {
        // The convention is: domain://tenant/namespace/topic
        // We want to persist in the order: tenant/namespace/domain/topic

        // For legacy naming scheme, the convention is: domain://tenant/cluster/namespace/topic
        // We want to persist in the order: tenant/cluster/namespace/domain/topic
        if (isV2()) {
            return String.format("%s/%s/%s/%s", tenant, namespacePortion, domain, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", tenant, cluster, namespacePortion, domain, getEncodedLocalName());
        }
    }

    /**
     * Get a string suitable for completeTopicName lookup.
     *
     * <p>Example:
     *
     * <p>persistent://tenant/cluster/namespace/completeTopicName ->
     *   persistent/tenant/cluster/namespace/completeTopicName
     *
     * @return
     */
    public String getLookupName() {
        if (isV2()) {
            return String.format("%s/%s/%s/%s", domain, tenant, namespacePortion, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", domain, tenant, cluster, namespacePortion, getEncodedLocalName());
        }
    }

    public boolean isGlobal() {
        return cluster == null || Constants.GLOBAL_CLUSTER.equalsIgnoreCase(cluster);
    }

    public String getSchemaName() {
        return getTenant()
            + "/" + getNamespacePortion()
            + "/" + TopicName.get(getPartitionedTopicName()).getEncodedLocalName();
    }

    @Override
    public String toString() {
        return completeTopicName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TopicName) {
            TopicName other = (TopicName) obj;
            return Objects.equals(completeTopicName, other.completeTopicName);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return completeTopicName.hashCode();
    }

    @Override
    public boolean includes(TopicName otherTopicName) {
        return this.equals(otherTopicName);
    }

    /**
     * Returns true if this a V2 topic name prop/ns/topic-name.
     * @return true if V2
     */
    public boolean isV2() {
        return cluster == null;
    }
}
