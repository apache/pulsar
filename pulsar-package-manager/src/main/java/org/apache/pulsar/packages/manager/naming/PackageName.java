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
package org.apache.pulsar.packages.manager.naming;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Getter;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A package name has four parts, type, namespace, package-name, and version.
 * And there are three type of packages, function, sink, and source. A package
 * name is format as type://namespace/name@version.
 */
public class PackageName {
    @Getter
    private final PackageType packageType;
    @Getter
    private final NamespaceName namespaceName;
    @Getter
    private final String name;
    @Getter
    private final String version;
    @Getter
    private final String completePackageName;
    @Getter
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
        this.packageType = PackageType.getEnum(parts.get(0));

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
        TopicName n = TopicName.get(parts.get(0));
        this.namespaceName = n.getNamespaceObject();
        this.name = n.getLocalName();
        this.version = Strings.isNullOrEmpty(parts.get(1)) ? "latest" : parts.get(1);
        this.completeName = String.format("%s/%s", namespaceName.toString(), name);
        this.completePackageName =
            String.format("%s://%s/%s@%s", packageType.toString(), namespaceName.toString(), name, version);
    }

    public String toFilePath() {
        return "";
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
        return another.packageType.equals(this.packageType)
            && another.completeName.equals(this.completeName)
            && another.version.equals(this.version);
    }
}
