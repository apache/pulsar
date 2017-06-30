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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Objects;

public class NamespaceName implements ServiceUnitId {

    private final String namespace;

    private String property;
    private String cluster;
    private String localName;

    public NamespaceName(String namespace) {
        try {
            checkNotNull(namespace);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid null namespace: " + namespace);
        }

        // Verify it's a proper namespace
        validateNamespaceName(namespace);
        this.namespace = namespace;
    }

    public NamespaceName(String property, String cluster, String namespace) {
        validateNamespaceName(property, cluster, namespace);
        this.namespace = property + '/' + cluster + '/' + namespace;
        this.property = property;
        this.cluster = cluster;
        this.localName = namespace;
    }

    public String getProperty() {
        return property;
    }

    public String getCluster() {
        return cluster;
    }

    public String getLocalName() {
        return localName;
    }

    public boolean isGlobal() {
        return "global".equals(cluster);
    }

    public String getPersistentTopicName(String localTopic) {
        return getDestinationName(DestinationDomain.persistent, localTopic);
    }

    /**
     * Compose the destination name from namespace + destination
     *
     * @param domain
     * @param destination
     * @return
     */
    String getDestinationName(DestinationDomain domain, String destination) {
        try {
            checkNotNull(domain);
            NamedEntity.checkName(destination);
            return String.format("%s://%s/%s", domain.toString(), namespace, destination);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Null pointer is invalid as domain for destination.", e);
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

    private void validateNamespaceName(String namespace) {
        // assume the namespace is in the form of <property>/<cluster>/<namespace>
        try {
            checkNotNull(namespace);
            String testUrl = String.format("http://%s", namespace);
            URI uri = new URI(testUrl);
            checkNotNull(uri.getPath());
            NamedEntity.checkURI(uri, testUrl);

            String[] parts = uri.getPath().split("/");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace);
            }
            validateNamespaceName(uri.getHost(), parts[1], parts[2]);

            property = uri.getHost();
            cluster = parts[1];
            localName = parts[2];
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace, e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Invalid namespace format. namespace: " + namespace, e);
        }
    }

    @Override
    public NamespaceName getNamespaceObject() {
        return this;
    }

    @Override
    public boolean includes(DestinationName dn) {
        return this.equals(dn.getNamespaceObject());
    }
}
