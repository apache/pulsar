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

package pkg.management;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;
import pkg.management.naming.PkgName;

/**
 * Packages provides a way to manage the packages of function, sink, source.
 */
public interface Packages {

    /**
     * Get the metadata of a package.
     *
     * @param pkgName package name
     * @return
     */
    CompletableFuture<PkgMetadata> getMeta(PkgName pkgName);

    /**
     * Update the metadata of a package.
     *
     * @param pkgName package name
     * @param metadata
     * @return
     */
    CompletableFuture<Void> updateMeta(PkgName pkgName, PkgMetadata metadata);

    /**
     * Download a package of a given version to a given path.
     *
     * @param pkgName package name
     * @param outputStream
     * @return
     */
    CompletableFuture<Void> download(PkgName pkgName, OutputStream outputStream);

    /**
     * Upload a package of a given version from a given path.
     *
     * @param pkgName  package name
     * @param metadata metadata of a package
     * @param inputStream
     * @return
     */
    CompletableFuture<Void> upload(PkgName pkgName, PkgMetadata metadata, InputStream inputStream);

    /**
     * Delete a package.
     *
     * It will delete all versions of a package if the version is not specified.
     * Otherwise it will delete the specified version package.
     *
     * @param pkgName package name
     *            type://tenant/namespace/name@version is delete a given version of the package
     *            type://tenant/namespace/name is delete all versions of the package
     * @return
     */
    CompletableFuture<Void> delete(PkgName pkgName);

    /**
     * List all the versions of a package.
     *
     * @param pkgName package name without version
     * @return
     */
    CompletableFuture<List<PkgName>> list(PkgName pkgName);

    /**
     * List all the packages of a namespace.
     *
     * @param namespace namespace name
     * @return
     */
    CompletableFuture<List<PkgName>> list(NamespaceName namespace);
}
