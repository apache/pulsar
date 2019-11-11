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

package pkg.management.naming;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.naming.NamespaceName;

public class PkgName {
    private PkgType type;
    private NamespaceName namespaceName;
    private String name;
    private String version;

    private String completeName;

    private static final LoadingCache<String, PkgName> cache = CacheBuilder.newBuilder().maximumSize(100000)
                                                                           .expireAfterAccess(30, TimeUnit.MINUTES)
                                                                           .build(new CacheLoader<String, PkgName>() {
                                                                               @Override
                                                                               public PkgName load(String name)
                                                                                   throws Exception {
                                                                                   return new PkgName(name);
                                                                               }
                                                                           });

    public static PkgName get(String type, String tenant, String namespace, String name, String version) {
        String pkgName = type + "://" + tenant + "/" + namespace + "/" + name + "@" + version;
        return get(pkgName);
    }

    public static PkgName get(String type, String name, String version) {
        String pkgName = type + "://" + name + "@" + version;
        return get(pkgName);
    }

    public static PkgName get(String packageName) {
        try {
            return cache.get(packageName);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    private PkgName(String packageName) {
        if (!packageName.contains("://")) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }

        List<String> parts = Splitter.on("://").limit(2).splitToList(packageName);
        this.type = PkgType.getEnum(parts.get(0));

        String rest = parts.get(1);
        parts = Splitter.on("/").splitToList(rest);
        if (parts.size() != 3) {
            throw new IllegalArgumentException("Invalid package name '" + packageName + "'");
        }
        this.namespaceName = NamespaceName.get(parts.get(0), parts.get(1));

        rest = parts.get(2);
        parts = Splitter.on("@").splitToList(rest);
        this.name = parts.get(0);
        this.version = parts.get(1);

        this.completeName = String.format("%s://%s/%s@%s", type.toString(), namespaceName.toString(), name, version);
    }

    public PkgType getPkgType() {
        return this.type;
    }

    public NamespaceName getNamespaceName() {
        return this.namespaceName;
    }

    public String getVersion() {
        return this.version;
    }

    public String getName() {
        return this.name;
    }

    public String getMetadataPath() {
        return String.format("%s/%s/%s/meta", this.type.toString(), this.namespaceName.toString(), this.name);
    }

    public String getRestPath() {
        return String.format("%s/%s/%s/%s", type.toString(), namespaceName.toString(), name, version);
    }

    public String getPathWithoutVersion() {
        return String.format("%s/%s/%s", type.toString(), namespaceName.toString(), name);
    }

    public String toString() {
        return completeName;
    }
}
