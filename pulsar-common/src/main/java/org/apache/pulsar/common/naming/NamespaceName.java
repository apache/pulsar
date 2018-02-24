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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class NamespaceName implements ServiceUnitId {

    private final String namespace;

    private final String property;
    private final String cluster;
    private final String localName;

    private static final LoadingCache<String, NamespaceName> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, NamespaceName>() {
                @Override
                public NamespaceName load(String name) throws Exception {
                    return new NamespaceName(name);
                }
            });

    public static NamespaceName get(String property, String namespace) {
        validateNamespaceName(property, namespace);
        return get(property + '/' + namespace);
    }

    public static NamespaceName get(String property, String cluster, String namespace) {
        validateNamespaceName(property, cluster, namespace);
        return get(property + '/' + cluster + '/' + namespace);
    }

    public static NamespaceName get(String namespace) {
        try {
            checkNotNull(namespace);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid null namespace: " + namespace);
        }
        try {
            return cache.get(namespace);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        } catch (UncheckedExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    private NamespaceName(String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("Invalid null namespace: " + namespace);
        }

        // Verify it's a proper namespace
        // The namespace name is composed of <property>/<namespace>
        // or in the legacy format with the cluster name:
        // <property>/<cluster>/<namespace>
        try {

            String[] parts = namespace.split("/");
            if (parts.length == 2) {
                // New style namespace : <property>/<namespace>
                validateNamespaceName(parts[0], parts[1]);

                property = parts[0];
                cluster = null;
                localName = parts[1];
            } else if (parts.length == 3) {
                // Old style namespace: <property>/<cluster>/<namespace>
                validateNamespaceName(parts[0], parts[1], parts[2]);

                property = parts[0];
                cluster = parts[1];
                localName = parts[2];
            } else {
                throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace);
            }
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace, e);
        }
        this.namespace = namespace;
    }

    public String getProperty() {
        return property;
    }

    @Deprecated
    public String getCluster() {
        return cluster;
    }

    public String getLocalName() {
        return localName;
    }

    public boolean isGlobal() {
        return cluster == null || Constants.GLOBAL_CLUSTER.equalsIgnoreCase(cluster);
    }

    public String getPersistentTopicName(String localTopic) {
        return getTopicName(TopicDomain.persistent, localTopic);
    }

    /**
     * Compose the topic name from namespace + topic
     *
     * @param domain
     * @param topic
     * @return
     */
    String getTopicName(TopicDomain domain, String topic) {
        try {
            checkNotNull(domain);
            NamedEntity.checkName(topic);
            return String.format("%s://%s/%s", domain.toString(), namespace, topic);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Null pointer is invalid as domain for topic.", e);
        }
    }

    @Override
    public String toString() {
        return namespace;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NamespaceName) {
            NamespaceName other = (NamespaceName) obj;
            return Objects.equal(namespace, other.namespace);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return namespace.hashCode();
    }

    public static void validateNamespaceName(String property, String namespace) {
        try {
            checkNotNull(property);
            checkNotNull(namespace);
            if (property.isEmpty() || namespace.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Invalid namespace format. namespace: %s/%s", property, namespace));
            }
            NamedEntity.checkName(property);
            NamedEntity.checkName(namespace);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid namespace format. namespace: %s/%s/%s", property, namespace), e);
        }
    }

    public static void validateNamespaceName(String property, String cluster, String namespace) {
        try {
            checkNotNull(property);
            checkNotNull(cluster);
            checkNotNull(namespace);
            if (property.isEmpty() || cluster.isEmpty() || namespace.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Invalid namespace format. namespace: %s/%s/%s", property, cluster, namespace));
            }
            NamedEntity.checkName(property);
            NamedEntity.checkName(cluster);
            NamedEntity.checkName(namespace);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid namespace format. namespace: %s/%s/%s", property, cluster, namespace), e);
        }
    }

    @Override
    public NamespaceName getNamespaceObject() {
        return this;
    }

    @Override
    public boolean includes(TopicName topicName) {
        return this.equals(topicName.getNamespaceObject());
    }
}
