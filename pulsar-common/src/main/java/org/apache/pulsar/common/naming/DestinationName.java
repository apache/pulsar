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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
 * Encapsulate the parsing of the destination name.
 */
public class DestinationName implements ServiceUnitId {

    private static final Logger log = LoggerFactory.getLogger(DestinationName.class);

    private static final String PARTITIONED_TOPIC_SUFFIX = "-partition-";

    private final String destination;

    private final DestinationDomain domain;
    private final String property;
    private final String cluster;
    private final String namespacePortion;
    private final String localName;

    private final NamespaceName namespaceName;

    private final int partitionIndex;

    private static final LoadingCache<String, DestinationName> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .build(new CacheLoader<String, DestinationName>() {
                @Override
                public DestinationName load(String name) throws Exception {
                    return new DestinationName(name);
                }
            });

    public static DestinationName get(String domain, String property, String cluster, String namespace,
            String destination) {
        String name = domain + "://" + property + '/' + cluster + '/' + namespace + '/' + destination;
        return DestinationName.get(name);
    }

    public static DestinationName get(String destination) {
        try {
            return cache.get(destination);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    public static boolean isValid(String destination) {
        try {
            get(destination);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private DestinationName(String destination) {
        this.destination = destination;
        try {
            // persistent://property/cluster/namespace/topic
            if (!destination.contains("://")) {
                throw new IllegalArgumentException(
                        "Invalid destination name: " + destination + " -- Domain is missing");
            }

            List<String> parts = Splitter.on("://").limit(2).splitToList(destination);
            this.domain = DestinationDomain.valueOf(parts.get(0));

            String rest = parts.get(1);
            // property/cluster/namespace/<localName>
            // Examples of localName:
            // 1. some/name/xyz//
            // 2. /xyz-123/feeder-2
            parts = Splitter.on("/").limit(4).splitToList(rest);
            if (parts.size() != 4) {
                throw new IllegalArgumentException("Invalid destination name: " + destination);
            }

            this.property = parts.get(0);
            this.cluster = parts.get(1);
            this.namespacePortion = parts.get(2);
            this.localName = parts.get(3);
            this.partitionIndex = getPartitionIndex(destination);

            NamespaceName.validateNamespaceName(property, cluster, namespacePortion);
            if (checkNotNull(localName).isEmpty()) {
                throw new IllegalArgumentException("Invalid destination name: " + destination);
            }
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid destination name: " + destination, e);
        }
        namespaceName = new NamespaceName(property, cluster, namespacePortion);
    }

    /**
     * Extract the namespace portion out of a destination name.
     *
     * Works both with old & new convention.
     *
     * @return the namespace
     */
    public String getNamespace() {
        return namespaceName.toString();
    }

    /**
     * Get the namespace object that this destination belongs to
     *
     * @return namespace object
     */
    @Override
    public NamespaceName getNamespaceObject() {
        return namespaceName;
    }

    public DestinationDomain getDomain() {
        return domain;
    }

    public String getProperty() {
        return property;
    }

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

    public DestinationName getPartition(int index) {
        if (this.toString().contains(PARTITIONED_TOPIC_SUFFIX)) {
            return this;
        }
        String partitionName = this.toString() + PARTITIONED_TOPIC_SUFFIX + index;
        return get(partitionName);
    }

    /**
     * @return partition index of the destination. It returns -1 if the destination (topic) is not partitioned.
     */
    public int getPartitionIndex() {
        return partitionIndex;
    }

    /**
     * @return partition index of the destination. It returns -1 if the destination (topic) is not partitioned.
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
     * Returns the name of the persistence resource associated with the destination.
     *
     * @return the relative path to be used in persistence
     */
    public String getPersistenceNamingEncoding() {
        // The convention is: domain://property/cluster/namespace/destination
        // We want to persist in the order: property/cluster/namespace/domain/destination
        return String.format("%s/%s/%s/%s/%s", property, cluster, namespacePortion, domain, getEncodedLocalName());
    }

    /**
     * Get a string suitable for destination lookup
     * <p>
     * Example:
     * <p>
     * persistent://property/cluster/namespace/destination -> persistent/property/cluster/namespace/destination
     *
     * @return
     */
    public String getLookupName() {
        return String.format("%s/%s/%s/%s/%s", domain, property, cluster, namespacePortion, getEncodedLocalName());
    }

    public boolean isGlobal() {
        return "global".equals(cluster);
    }

    @Override
    public String toString() {
        return destination;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DestinationName) {
            DestinationName other = (DestinationName) obj;
            return Objects.equal(destination, other.destination);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return destination.hashCode();
    }

    @Override
    public boolean includes(DestinationName dn) {
        return this.equals(dn);
    }

}
