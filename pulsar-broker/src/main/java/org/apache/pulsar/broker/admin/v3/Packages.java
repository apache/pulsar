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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.InputStream;
import org.apache.pulsar.broker.admin.impl.PackagesBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.glassfish.jersey.media.multipart.FormDataParam;

@Path("/packages")
@Tag(name = "packages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Packages extends PackagesBase {

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}/metadata")
    @Operation(
        summary = "Get the metadata of a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Return the metadata of the specified package.",
                content = @Content(schema = @Schema(implementation = PackageMetadata.class))),
            @ApiResponse(responseCode = "404", description = "The specified package is not existent."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    public void getMeta(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        @Suspended AsyncResponse asyncResponse
    ) {
        internalGetMetadata(type, tenant, namespace, packageName, version, asyncResponse);
    }

    @PUT
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}/metadata")
    @Operation(
        summary = "Update the metadata of a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "204", description = "Update the metadata of the specified package "
                + "successfully."),
            @ApiResponse(responseCode = "404", description = "The specified package is not existent."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    @Consumes(MediaType.APPLICATION_JSON)
    public void updateMeta(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        final PackageMetadata metadata,
        @Suspended AsyncResponse asyncResponse
    ) {
        if (metadata != null) {
            metadata.setModificationTime(System.currentTimeMillis());
            internalUpdateMetadata(type, tenant, namespace, packageName, version, metadata, asyncResponse);
        } else {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, "Unknown error, metadata is "
                + "null when processing update package metadata request"));
        }
    }

    @POST
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    @Operation(
        summary = "Upload a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "204", description = "Upload the specified package successfully."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void upload(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        final @FormDataParam("metadata") PackageMetadata packageMetadata,
        final @FormDataParam("file") InputStream uploadedInputStream,
        @Suspended AsyncResponse asyncResponse) {
        if (packageMetadata != null) {
            packageMetadata.setCreateTime(System.currentTimeMillis());
            packageMetadata.setModificationTime(System.currentTimeMillis());
            internalUpload(type, tenant, namespace, packageName, version, packageMetadata,
                uploadedInputStream, asyncResponse);
        } else {
            asyncResponse.resume(new RestException(Response.Status.BAD_REQUEST, "Unknown error, metadata is "
                + "null when processing update package metadata request"));
        }
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    @Operation(
        summary = "Download a package with the package name."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Download the specified package successfully.",
                content = @Content(mediaType = "application/octet-stream",
                    schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "404", description = "The specified package is not existent."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    public StreamingOutput download(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version
        ) {
        return internalDownload(type, tenant, namespace, packageName, version);
    }

    @DELETE
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}")
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "204", description = "Delete the specified package successfully."),
            @ApiResponse(responseCode = "404", description = "The specified package is not existent."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    @Operation(summary = "Delete a package with the package name.")
    public void delete(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        final @PathParam("version") String version,
        @Suspended AsyncResponse asyncResponse
    ){
        internalDelete(type, tenant, namespace, packageName, version, asyncResponse);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}")
    @Operation(
        summary = "Get all the versions of a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description = "Return the package versions of the specified package.",
                content = @Content(array = @ArraySchema(schema = @Schema(implementation = String.class)))),
            @ApiResponse(responseCode = "404", description = "The specified package is not existent."),
            @ApiResponse(responseCode = "412", description = "The package name is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    public void listPackageVersion(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        final @PathParam("packageName") String packageName,
        @Suspended AsyncResponse asyncResponse
    ) {
        internalListVersions(type, tenant, namespace, packageName, asyncResponse);
    }

    @GET
    @Path("/{type}/{tenant}/{namespace}")
    @Operation(
        summary = "Get all the specified type packages in a namespace."
    )
    @ApiResponses(
        value = {
            @ApiResponse(responseCode = "200", description =
                "Return all the specified type package names in the specified namespace.",
                content = @Content(array = @ArraySchema(schema = @Schema(implementation = PackageMetadata.class)))),
            @ApiResponse(responseCode = "412", description = "The package type is illegal."),
            @ApiResponse(responseCode = "500", description = "Internal server error."),
            @ApiResponse(responseCode = "503", description = "Package Management Service is not enabled in the broker.")
        }
    )
    public void listPackages(
        final @PathParam("type") String type,
        final @PathParam("tenant") String tenant,
        final @PathParam("namespace") String namespace,
        @Suspended AsyncResponse asyncResponse
    ) {
        internalListPackages(type, tenant, namespace, asyncResponse);
    }
}
