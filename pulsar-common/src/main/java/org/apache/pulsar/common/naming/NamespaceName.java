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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Parser of a value from the namespace field provided in configuration.
 */
public class NamespaceName implements ServiceUnitId {

    private final String namespace;

    private final String tenant;
    private final String cluster;
    private final String localName;

    private static final LoadingCache<String, NamespaceName> cache = CacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES).build(new CacheLoader<String, NamespaceName>() {
                @Override
                public NamespaceName load(String name) throws Exception {
                    return new NamespaceName(name);
                }
            });

    public static final NamespaceName SYSTEM_NAMESPACE = NamespaceName.get("pulsar/system");

    public static NamespaceName get(String tenant, String namespace) {
        validateNamespaceName(tenant, namespace);
        return get(tenant + '/' + namespace);
    }

    public static NamespaceName get(String tenant, String cluster, String namespace) {
        validateNamespaceName(tenant, cluster, namespace);
        return get(tenant + '/' + cluster + '/' + namespace);
    }

    public static NamespaceName get(String namespace) {
        if (namespace == null || namespace.isEmpty()) {
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

    public static Optional<NamespaceName> getIfValid(String namespace) {
        NamespaceName ns = cache.getIfPresent(namespace);
        if (ns != null) {
            return Optional.of(ns);
        }

        if (namespace.length() == 0) {
            return Optional.empty();
        }

        // Example: my-tenant/my-namespace
        if (!namespace.contains("/")) {
            return Optional.empty();
        }

        return Optional.of(get(namespace));
    }

    private NamespaceName(String namespace) {
        // Verify it's a proper namespace
        // The namespace name is composed of <tenant>/<namespace>
        // or in the legacy format with the cluster name:
        // <tenant>/<cluster>/<namespace>
        try {

            String[] parts = namespace.split("/");
            if (parts.length == 2) {
                // New style namespace : <tenant>/<namespace>
                validateNamespaceName(parts[0], parts[1]);

                tenant = parts[0];
                cluster = null;
                localName = parts[1];
            } else if (parts.length == 3) {
                // Old style namespace: <tenant>/<cluster>/<namespace>
                validateNamespaceName(parts[0], parts[1], parts[2]);

                tenant = parts[0];
                cluster = parts[1];
                localName = parts[2];
            } else {
                throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace);
            }
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalArgumentException("Invalid namespace format."
                    + " expected <tenant>/<namespace> or <tenant>/<cluster>/<namespace> "
                    + "but got: " + namespace, e);
        }
        this.namespace = namespace;
    }

    public String getTenant() {
        return tenant;
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
     * Compose the topic name from namespace + topic.
     *
     * @param domain
     * @param topic
     * @return
     */
    String getTopicName(TopicDomain domain, String topic) {
        if (domain == null) {
            throw new IllegalArgumentException("invalid null domain");
        }
        NamedEntity.checkName(topic);
        return String.format("%s://%s/%s", domain.toString(), namespace, topic);
    }

    @Override
    public String toString() {
        return namespace;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NamespaceName) {
            NamespaceName other = (NamespaceName) obj;
            return Objects.equals(namespace, other.namespace);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return namespace.hashCode();
    }

    public static void validateNamespaceName(String tenant, String namespace) {
        if ((tenant == null || tenant.isEmpty()) || (namespace == null || namespace.isEmpty())) {
            throw new IllegalArgumentException(
                    String.format("Invalid namespace format. namespace: %s/%s", tenant, namespace));
        }
        NamedEntity.checkName(tenant);
        NamedEntity.checkName(namespace);
    }

    public static void validateNamespaceName(String tenant, String cluster, String namespace) {
        if ((tenant == null || tenant.isEmpty()) || (cluster == null || cluster.isEmpty())
                || (namespace == null || namespace.isEmpty())) {
            throw new IllegalArgumentException(
                    String.format("Invalid namespace format. namespace: %s/%s/%s", tenant, cluster, namespace));
        }
        NamedEntity.checkName(tenant);
        NamedEntity.checkName(cluster);
        NamedEntity.checkName(namespace);
    }

    @Override
    public NamespaceName getNamespaceObject() {
        return this;
    }

    @Override
    public boolean includes(TopicName topicName) {
        return this.equals(topicName.getNamespaceObject());
    }

    /**
     * Returns true if this is a V2 namespace prop/namespace-name.
     * @return true if v2
     */
    public boolean isV2() {
        return cluster == null;
    }
}
