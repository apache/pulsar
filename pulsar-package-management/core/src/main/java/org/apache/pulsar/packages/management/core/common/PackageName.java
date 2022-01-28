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
package org.apache.pulsar.packages.management.core.common;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A package name has five parts, type, tenant, namespace, package-name, and version.
 * And there are three type of packages, function, sink, and source. A package
 * name is format as type://tenant/namespace/name@version.
 */
public class PackageName {
    private final PackageType type;
    // If we use NamespaceName, that will cause an cycle reference when we use this package at pulsar service for
    // REST API handlers.
    private final String namespace;
    private final String tenant;
    private final String name;
    private final String version;
    private final String completePackageName;
    private final String completeName;

    private static final LoadingCache<String, PackageName> cache =
        CacheBuilder.newBuilder()
            .maximumSize(100000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(new CacheLoader<String, PackageName>() {
                @Override
                public PackageName load(String name) throws Exception {
                    return new PackageName(name);
                }
            });

    public static PackageName get(String type, String tenant, String namespace, String name, String version) {
        String pkgName = type + "://" + tenant + "/" + namespace + "/" + name + "@" + version;
        return get(pkgName);
    }

    public static PackageName get(String type, String name, String version) {
        String pkgName = type + "://" + name + "@" + version;
        return get(pkgName);
    }

    public static PackageName get(String packageName) {
        try {
            return cache.get(packageName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private PackageName(String packageName) {
        if (!packageName.contains("://")) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }

        List<String> parts = Splitter.on("://").limit(2).splitToList(packageName);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }
        this.type = PackageType.getEnum(parts.get(0));

        String rest = parts.get(1);
        // if the package name does not contains '@', that means user does not set the version of package.
        // We will set the version to latest.
        if (!rest.contains("@")) {
            rest += "@";
        }
        parts = Splitter.on("@").splitToList(rest);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }
        List<String> partsWithoutVersion = Splitter.on("/").splitToList(parts.get(0));
        if (partsWithoutVersion.size() != 3) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }
        this.tenant = partsWithoutVersion.get(0);
        this.namespace = partsWithoutVersion.get(1);
        this.name = partsWithoutVersion.get(2);
        this.version = Strings.isNullOrEmpty(parts.get(1)) ? "latest" : parts.get(1);
        this.completeName = String.format("%s/%s/%s", tenant, namespace, name);
        this.completePackageName =
            String.format("%s://%s/%s/%s@%s", type.toString(), tenant, namespace, name, version);
    }

    public PackageType getPkgType() {
        return this.type;
    }

    public String getTenant() {
        return this.tenant;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public String getVersion() {
        return this.version;
    }

    public String getName() {
        return this.name;
    }

    public String getCompleteName() {
        return this.completeName;
    }

    public String toString() {
        return completePackageName;
    }

    public String toRestPath() {
        return String.format("%s/%s/%s/%s/%s", type, tenant, namespace, name, version);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PackageName another = (PackageName) obj;
        return another.type.equals(this.type)
            && another.completeName.equals(this.completeName)
            && another.version.equals(this.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, namespace, tenant, name, version);
    }
}
