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

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.migration.MigrationState;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.coordination.impl.MigrationCoordinator;
import org.apache.pulsar.metadata.impl.DualMetadataStore;

/**
 * Admin resource for metadata store migration operations.
 */
@Slf4j
public class MetadataMigrationBase extends AdminResource {

    @GET
    @Path("/status")
    @ApiOperation(value = "Get current migration status", response = MigrationState.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Migration status retrieved successfully"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public MigrationState getStatus() {
        validateSuperUserAccess();

        try {
            var ogr = pulsar().getLocalMetadataStore().get(MigrationState.MIGRATION_FLAG_PATH).get();
            if (ogr.isPresent()) {
                return ObjectMapperFactory.getMapper().reader().readValue(ogr.get().getValue(), MigrationState.class);
            } else {
                return MigrationState.NOT_STARTED;
            }
        } catch (Exception e) {
            log.error("Failed to get migration status", e);
            throw new RestException(e);
        }
    }

    @POST
    @Path("/start")
    @ApiOperation(value = "Start metadata store migration")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "Migration started successfully"),
            @ApiResponse(code = 400, message = "Invalid target URL"),
            @ApiResponse(code = 409, message = "Migration already in progress"),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    public void startMigration(
            @ApiParam(value = "Target metadata store URL", required = true)
            @QueryParam("target")
            String targetUrl) {
        validateSuperUserAccess();

        if (targetUrl == null || targetUrl.trim().isEmpty()) {
            throw new RestException(Response.Status.BAD_REQUEST, "Target URL is required");
        }

        try {
            // Check if metadata store is wrapped with DualMetadataStore
            if (!(pulsar().getLocalMetadataStore() instanceof DualMetadataStore)) {
                throw new RestException(Response.Status.BAD_REQUEST, "Metadata store is not configured for migration. "
                        + "Please ensure you're using a supported source metadata store (e.g., ZooKeeper).");
            }

            // Create coordinator
            MigrationCoordinator coordinator = new MigrationCoordinator(pulsar().getLocalMetadataStore(), targetUrl);

            // Start migration in background thread
            pulsar().getExecutor().submit(() -> {
                try {
                    log.info("Starting metadata migration to: {}", targetUrl);
                    coordinator.startMigration();
                    log.info("Metadata migration completed successfully");
                } catch (Exception e) {
                    log.error("Metadata migration failed", e);
                }
            });

            log.info("Migration initiated to target: {}", targetUrl);

        } catch (RestException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to start migration", e);
            throw new RestException(e);
        }
    }
}
