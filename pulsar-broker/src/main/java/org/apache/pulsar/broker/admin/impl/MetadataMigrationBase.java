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
package org.apache.pulsar.broker.admin.impl;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.coordination.impl.MigrationCoordinator;
import org.apache.pulsar.metadata.impl.DualMetadataStore;

/**
 * Admin resource for metadata store migration operations.
 */
public class MetadataMigrationBase extends AdminResource {

    @GET
    @Path("/status")
    @Operation(summary = "Get current migration status")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Migration status retrieved successfully",
                    content = @Content(schema = @Schema(implementation = MigrationState.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public MigrationState getStatus() {
        validateSuperUserAccess();

        try {
            // The migration flag lives in the source store. Don't read it through the
            // DualMetadataStore: once the migration is completed, its reads are routed to the
            // target store, which doesn't hold the flag.
            MetadataStore store = pulsar().getLocalMetadataStore();
            if (store instanceof DualMetadataStore dualStore) {
                store = dualStore.getSourceStore();
            }
            var ogr = store.get(MigrationState.MIGRATION_FLAG_PATH).get();
            if (ogr.isPresent()) {
                return ObjectMapperFactory.getMapper().reader().readValue(ogr.get().getValue(), MigrationState.class);
            } else {
                return MigrationState.NOT_STARTED;
            }
        } catch (Exception e) {
            log.error().exception(e).log("Failed to get migration status");
            throw new RestException(e);
        }
    }

    @POST
    @Path("/start")
    @Operation(summary = "Start metadata store migration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Migration started successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid target URL"),
            @ApiResponse(responseCode = "409", description = "Migration already in progress"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public void startMigration(
            @Parameter(description = "Target metadata store URL", required = true)
            @QueryParam("target")
            String targetUrl) {
        validateSuperUserAccess();

        if (targetUrl == null || targetUrl.trim().isEmpty()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Target URL is required");
        }

        try {
            // Check if metadata store is wrapped with DualMetadataStore
            if (!(pulsar().getLocalMetadataStore() instanceof DualMetadataStore dualStore)) {
                throw new RestException(Response.Status.BAD_REQUEST, "Metadata store is not configured for migration. "
                        + "Please ensure you're using a supported source metadata store (e.g., ZooKeeper).");
            }

            // Reject the request if a migration is already in progress or was completed. The migration
            // flag is always kept in the source store, so read it from there: after a completed
            // migration the dual store would route the read to the target store.
            var existingFlag = dualStore.getSourceStore().get(MigrationState.MIGRATION_FLAG_PATH).get();
            if (existingFlag.isPresent()) {
                MigrationState currentState = ObjectMapperFactory.getMapper().reader()
                        .readValue(existingFlag.get().getValue(), MigrationState.class);
                switch (currentState.getPhase()) {
                    case PREPARATION, COPYING -> throw new RestException(Response.Status.CONFLICT,
                            "Migration is already in progress (phase: " + currentState.getPhase() + ")");
                    case COMPLETED -> throw new RestException(Response.Status.CONFLICT,
                            "Migration has already been completed");
                    default -> {
                        // NOT_STARTED or FAILED: ok to start (or retry) the migration
                    }
                }
            }

            // Create coordinator
            MigrationCoordinator coordinator = new MigrationCoordinator(pulsar().getLocalMetadataStore(), targetUrl);

            // Start migration in background thread
            pulsar().getExecutor().submit(() -> {
                try {
                    log.info().attr("targetUrl", targetUrl).log("Starting metadata migration");
                    coordinator.startMigration();
                    log.info("Metadata migration completed successfully");
                } catch (Exception e) {
                    log.error().exception(e).log("Metadata migration failed");
                }
            });

            log.info().attr("targetUrl", targetUrl).log("Migration initiated");

        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error().exception(e).log("Failed to start migration");
            throw new RestException(e);
        }
    }
}
