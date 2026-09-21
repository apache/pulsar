/*
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
package org.apache.pulsar.client.cli;

import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public abstract class AbstractCmd implements Callable<Integer> {
    // Picocli entrypoint.
    @Override
    public Integer call() throws Exception {
        return run();
    }

    abstract int run() throws Exception;

    /**
     * Resolve a {@code file:} URI (as accepted by the encryption-key flags) to a {@link Path}.
     * Supports both the hierarchical form ({@code file:///abs/path}, where {@link URI#getPath()}
     * is set) and the opaque relative form ({@code file:rel/path}, where the path lives in the
     * scheme-specific part).
     *
     * @param fileUri a {@code file:} URI string
     * @return the resolved {@link Path}
     * @throws IllegalArgumentException if the URI scheme is not {@code file}
     */
    static Path fileUriToPath(String fileUri) {
        URI uri = URI.create(fileUri);
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("This version of pulsar-client supports only file:// "
                    + "encryption keys; got '" + fileUri + "'.");
        }
        String path = uri.getPath();
        if (path == null) {
            // Opaque (relative) file: URI, e.g. file:../certs/key.pem
            path = uri.getSchemeSpecificPart();
        }
        return Path.of(path);
    }
}
