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
package org.apache.pulsar.broker.admin.v2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.RawBookieInfo;

@Path("/bookies")
@Tag(name = "bookies", description = "Configure bookies rack placement")
@Produces(MediaType.APPLICATION_JSON)
@SuppressWarnings("deprecation")
public class Bookies extends AdminResource {
    private static final String PATH_SEPARATOR = "/";

    @GET
    @Path("/racks-info")
    @Operation(summary = "Gets the rack placement information for all the bookies in the cluster")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Gets the rack placement information for all the bookies in the cluster",
                    content = @Content(schema = @Schema(implementation = BookiesRackConfiguration.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void getBookiesRackInfo(@Suspended final AsyncResponse asyncResponse) {
        validateSuperUserAccess();

        getPulsarResources().getBookieResources().get()
                .thenAccept(b -> {
                    asyncResponse.resume(b.orElseGet(() -> new BookiesRackConfiguration()));
                }).exceptionally(ex -> {
            asyncResponse.resume(ex);
            return null;
        });
    }

    @GET
    @Path("/all")
    @Operation(summary = "Gets raw information for all the bookies in the cluster")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Gets raw information for all the bookies in the cluster",
                    content = @Content(schema = @Schema(implementation = BookiesClusterInfo.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public BookiesClusterInfo getAllBookies() throws Exception {
        validateSuperUserAccess();

        BookKeeper bookKeeper = bookKeeper();
        MetadataClientDriver metadataClientDriver = bookKeeper.getMetadataClientDriver();
        RegistrationClient registrationClient = metadataClientDriver.getRegistrationClient();

        Set<BookieId> allBookies = registrationClient.getAllBookies().get().getValue();
        List<RawBookieInfo> result = new ArrayList<>(allBookies.size());
        for (BookieId bookieId : allBookies) {
            RawBookieInfo bookieInfo = new RawBookieInfo(bookieId.toString());
            result.add(bookieInfo);
        }
        return BookiesClusterInfo.builder().bookies(result).build();
    }

    @GET
    @Path("/racks-info/{bookie}")
    @Operation(summary = "Gets the rack placement information for a specific bookie in the cluster")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200",
                    description = "Gets the rack placement information for a specific bookie in the cluster",
                    content = @Content(schema = @Schema(implementation = BookieInfo.class))),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")})
    public void getBookieRackInfo(@Suspended final AsyncResponse asyncResponse,
                                  @PathParam("bookie") String bookieAddress) throws Exception {
        validateSuperUserAccess();

        getPulsarResources().getBookieResources().get()
                .thenAccept(b -> {
                    Optional<BookieInfo> bi = b.orElseGet(() -> new BookiesRackConfiguration())
                            .getBookie(bookieAddress);
                    if (bi.isPresent()) {
                        asyncResponse.resume(bi.get());
                    } else {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Bookie rack placement configuration not found: " + bookieAddress));
                    }
                }).exceptionally(ex -> {
            asyncResponse.resume(ex);
            return null;
        });
    }

    @DELETE
    @Path("/racks-info/{bookie}")
    @Operation(summary = "Removed the rack placement information for a specific bookie in the cluster")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")
    })
    public void deleteBookieRackInfo(@Suspended final AsyncResponse asyncResponse,
                                     @PathParam("bookie") String bookieAddress) throws Exception {
        validateSuperUserAccess();

        getPulsarResources().getBookieResources()
                .update(optionalBookiesRackConfiguration -> {
                    BookiesRackConfiguration brc = optionalBookiesRackConfiguration
                            .orElseGet(() -> new BookiesRackConfiguration());

                    if (!brc.removeBookie(bookieAddress)) {
                        asyncResponse.resume(new RestException(Status.NOT_FOUND,
                                "Bookie rack placement configuration not found: " + bookieAddress));
                    }

                    return brc;
                }).thenAccept(__ -> {
            log.info().attr("bookieAddress", bookieAddress).log("Removed from rack mapping info");
            asyncResponse.resume(Response.noContent().build());
        }).exceptionally(ex -> {
            asyncResponse.resume(ex);
            return null;
        });
    }

    @POST
    @Path("/racks-info/{bookie}")
    @Operation(summary = "Updates the rack placement information for a specific bookie in the cluster (note."
            + " bookie address format:`address:port`)")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Operation successful"),
            @ApiResponse(responseCode = "403", description = "Don't have admin permission")}
    )
    public void updateBookieRackInfo(@Suspended final AsyncResponse asyncResponse,
                                     @Parameter(description = "The bookie address", required = true)
                                     @PathParam("bookie") String bookieAddress,
                                     @Parameter(description = "The group", required = true)
                                     @QueryParam("group") String group,
                                     @RequestBody(description = "The bookie info", required = true)
                                     BookieInfo bookieInfo) throws Exception {
        validateSuperUserAccess();

        if (group == null) {
            throw new RestException(Status.PRECONDITION_FAILED, "Bookie 'group' parameters is missing");
        }

        // validate rack name
        int separatorCnt = StringUtils.countMatches(
            StringUtils.strip(bookieInfo.getRack(), PATH_SEPARATOR), PATH_SEPARATOR);
        boolean isRackEnabled = pulsar().getConfiguration().isBookkeeperClientRackawarePolicyEnabled();
        boolean isRegionEnabled = pulsar().getConfiguration().isBookkeeperClientRegionawarePolicyEnabled();
        if (isRackEnabled && ((isRegionEnabled && separatorCnt != 1) || (!isRegionEnabled && separatorCnt != 0))) {
            asyncResponse.resume(new RestException(Status.PRECONDITION_FAILED, "Bookie 'rack' parameter is invalid, "
                + "When `RackawareEnsemblePlacementPolicy` is enabled, the rack name is not allowed to contain "
                + "slash (`/`) except for the beginning and end of the rack name string. "
                + "When `RegionawareEnsemblePlacementPolicy` is enabled, the rack name can only contain "
                + "one slash (`/`) except for the beginning and end of the rack name string."));
            return;
        }

        getPulsarResources().getBookieResources()
                .update(optionalBookiesRackConfiguration -> {
                    BookiesRackConfiguration brc = optionalBookiesRackConfiguration
                            .orElseGet(() -> new BookiesRackConfiguration());

                    brc.updateBookie(group, bookieAddress, bookieInfo);

                    return brc;
                }).thenAccept(__ -> {
            log.info().attr("bookieAddress", bookieAddress).log("Updated rack mapping info");
            asyncResponse.resume(Response.noContent().build());
        }).exceptionally(ex -> {
            asyncResponse.resume(ex);
            return null;
        });
    }
}
