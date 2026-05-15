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
package org.apache.pulsar.client.admin.internal;

import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.MetadataMigration;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.migration.MigrationState;

public class MetadataMigrationImpl extends BaseResource implements MetadataMigration {
    private final WebTarget adminPath;

    public MetadataMigrationImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        adminPath = web.path("/admin/v2/metadata/migration");
    }

    @Override
    public CompletableFuture<Void> start(String targetUrl) {

        return asyncPostRequest(adminPath.path("start")
                .queryParam("target", targetUrl), null).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<MigrationState> status() {
        return asyncGetRequest(adminPath.path("status"), new FutureCallback<>(){});
    }
}
