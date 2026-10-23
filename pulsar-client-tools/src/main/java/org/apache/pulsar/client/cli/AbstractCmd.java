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
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.pulsar.cli.ClientApi;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public abstract class AbstractCmd implements Callable<Integer> {

    /** Second paragraph of the description of every command that can drive both client APIs. */
    static final String CLIENT_API_DESCRIPTION = "The topic picks the client: topic:// (scalable) topics use "
            + "the V5 client; persistent://, non-persistent:// and unprefixed topics use the v4 client. "
            + ClientApi.OPTION_NAME + " overrides the choice. Options listed under a client's section apply "
            + "only when that client is used.";

    /** Hint appended to errors about a capability only the v4 client has. */
    static final String USE_V4_CLIENT_HINT = "use a persistent:// topic or " + ClientApi.OPTION_NAME + " V4";

    // Picocli entrypoint.
    @Override
    public Integer call() throws Exception {
        return run();
    }

    abstract int run() throws Exception;

    /**
     * Resolve the client API for {@code topic} and reject options typed on the command line that
     * belong to the other client. A WebSocket service URL goes through the WebSocket proxy, which
     * only serves persistent and non-persistent topics, so a {@code topic://} topic is rejected there.
     */
    static ClientApi resolveClientApi(CommandSpec spec, ClientApi requested, String topic, String serviceURL) {
        if (isWebSocketUrl(serviceURL) && ClientApi.isScalableTopic(topic)) {
            throw new ParameterException(spec.commandLine(), "Topic '" + topic + "' is a topic:// (scalable) "
                    + "topic, which the WebSocket proxy (" + serviceURL + ") does not support.");
        }
        ClientApi clientApi = ClientApi.resolve(requested, List.of(topic), spec.commandLine());
        ClientApiOptionGroups.validate(spec, clientApi);
        return clientApi;
    }

    static boolean isWebSocketUrl(String serviceURL) {
        return serviceURL != null && serviceURL.startsWith("ws");
    }

    /**
     * The V5 client only reads encryption keys from files, so reject any other key URI (such as
     * {@code data:}) up front when the V5 client is used.
     */
    static void validateV5EncryptionKeyUri(CommandSpec spec, String keyUri) {
        if (keyUri != null && !keyUri.isBlank() && !keyUri.regionMatches(true, 0, "file:", 0, 5)) {
            throw new ParameterException(spec.commandLine(), "--encryption-key-value '" + keyUri
                    + "': the V5 client supports only file: key URIs; for other key URIs " + USE_V4_CLIENT_HINT
                    + ".");
        }
    }

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
