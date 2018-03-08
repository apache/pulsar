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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.util.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Encapsulate the parsing of the completeTopicName name.
 */
public class TopicName implements ServiceUnitId {

    private static final Logger log = LoggerFactory.getLogger(TopicName.class);

    private static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private final String completeTopicName;

    private final TopicDomain domain;
    private final String property;
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

    public static TopicName get(String domain, NamespaceName namespaceName, String topic) {
        String name = domain + "://" + namespaceName.toString() + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String property, String namespace, String topic) {
        String name = domain + "://" + property + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String domain, String property, String cluster, String namespace,
            String topic) {
        String name = domain + "://" + property + '/' + cluster + '/' + namespace + '/' + topic;
        return TopicName.get(name);
    }

    public static TopicName get(String topic) {
        try {
            return cache.get(topic);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
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
        this.completeTopicName = completeTopicName;
        try {
            // The topic name can be in two different forms:
            // new:    persistent://property/namespace/topic
            // legacy: persistent://property/cluster/namespace/topic
            if (!completeTopicName.contains("://")) {
                throw new IllegalArgumentException(
                        "Invalid completeTopicName name: " + completeTopicName + " -- Domain is missing");
            }

            List<String> parts = Splitter.on("://").limit(2).splitToList(completeTopicName);
            this.domain = TopicDomain.getEnum(parts.get(0));

            String rest = parts.get(1);

            // The rest of the name can be in different forms:
            // new:    property/namespace/<localName>
            // legacy: property/cluster/namespace/<localName>
            // Examples of localName:
            // 1. some/name/xyz//
            // 2. /xyz-123/feeder-2


            parts = Splitter.on("/").limit(4).splitToList(rest);
            if (parts.size() == 3) {
                // New topic name without cluster name
                this.property = parts.get(0);
                this.cluster = null;
                this.namespacePortion = parts.get(1);
                this.localName = parts.get(2);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(property, namespacePortion);
            } else if (parts.size() == 4) {
                // Legacy topic name that includes cluster name
                this.property = parts.get(0);
                this.cluster = parts.get(1);
                this.namespacePortion = parts.get(2);
                this.localName = parts.get(3);
                this.partitionIndex = getPartitionIndex(completeTopicName);
                this.namespaceName = NamespaceName.get(property, cluster, namespacePortion);
            } else {
                throw new IllegalArgumentException("Invalid completeTopicName name: " + completeTopicName);
            }


            if (localName == null || localName.isEmpty()) {
                throw new IllegalArgumentException("Invalid completeTopicName name: " + completeTopicName);
            }
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid completeTopicName name: " + completeTopicName, e);
        }

    }

    /**
     * Extract the namespace portion out of a completeTopicName name.
     *
     * Works both with old & new convention.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespaceName.toString();
    }

    /**
     * Get the namespace object that this completeTopicName belongs to
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

    public String getProperty() {
        return property;
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
     * @return partition index of the completeTopicName. It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public int getPartitionIndex() {
        return partitionIndex;
    }

    public boolean isPartitioned() {
        return partitionIndex != -1;
    }

    /**
     * For partitions in a topic, return the base partitioned topic name
     * Eg:
     * <ul>
     *  <li><code>persistent://prop/cluster/ns/my-topic-partition-1</code> --> <code>persistent://prop/cluster/ns/my-topic</code>
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
     * @return partition index of the completeTopicName. It returns -1 if the completeTopicName (topic) is not partitioned.
     */
    public static int getPartitionIndex(String topic) {
        int partitionIndex = -1;
        if (topic.contains(PARTITIONED_TOPIC_SUFFIX)) {
            try {
                partitionIndex = Integer.parseInt(topic.substring(topic.lastIndexOf('-') + 1));
            } catch (NumberFormatException nfe) {
                log.warn("Could not get the partition index from the topic {}", topic);
            }
        }

        return partitionIndex;
    }

    /**
     * Returns the name of the persistence resource associated with the completeTopicName.
     *
     * @return the relative path to be used in persistence
     */
    public String getPersistenceNamingEncoding() {
        // The convention is: domain://property/namespace/topic
        // We want to persist in the order: property/namespace/domain/topic

        // For legacy naming scheme, the convention is: domain://property/cluster/namespace/topic
        // We want to persist in the order: property/cluster/namespace/domain/topic
        if (cluster == null) {
            return String.format("%s/%s/%s/%s", property, namespacePortion, domain, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", property, cluster, namespacePortion, domain, getEncodedLocalName());
        }
    }

    /**
     * Get a string suitable for completeTopicName lookup
     * <p>
     * Example:
     * <p>
     * persistent://property/cluster/namespace/completeTopicName -> persistent/property/cluster/namespace/completeTopicName
     *
     * @return
     */
    public String getLookupName() {
        if (cluster == null) {
            return String.format("%s/%s/%s/%s", domain, property, namespacePortion, getEncodedLocalName());
        } else {
            return String.format("%s/%s/%s/%s/%s", domain, property, cluster, namespacePortion, getEncodedLocalName());
        }
    }

    public boolean isGlobal() {
        return cluster == null || Constants.GLOBAL_CLUSTER.equalsIgnoreCase(cluster);
    }

    public String getSchemaName() {
        return getProperty()
            + "/" + getNamespacePortion()
            + "/" + getLocalName();
    }

    @Override
    public String toString() {
        return completeTopicName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TopicName) {
            TopicName other = (TopicName) obj;
            return Objects.equal(completeTopicName, other.completeTopicName);
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

}
