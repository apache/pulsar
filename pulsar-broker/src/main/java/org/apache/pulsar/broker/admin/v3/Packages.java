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
package org.apache.pulsar.broker.admin.v3;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.InputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.pulsar.broker.admin.impl.PackagesBase;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.glassfish.jersey.media.multipart.FormDataParam;

@Path("/packages")
@Api(value = "packages", tags = "packages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Packages extends PackagesBase {

    @GET
    @Path("/{type}/{tenant}/{namespace}/{packageName}/{version}/metadata")
    @ApiOperation(
        value = "Get the metadata of a package.",
        response = PackageMetadata.class
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message = "Return the metadata of the specified package."),
            @ApiResponse(code = 404, message = "The specified package is not existent."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
    @ApiOperation(
        value = "Update the metadata of a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message = "Update the metadata of the specified package successfully."),
            @ApiResponse(code = 404, message = "The specified package is not existent."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
    @ApiOperation(
        value = "Upload a package."
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message = "Upload the specified package successfully."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
    @ApiOperation(
        value = "Download a package with the package name.",
        response = StreamingOutput.class
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message = "Download the specified package successfully."),
            @ApiResponse(code = 404, message = "The specified package is not existent."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
            @ApiResponse(code = 200, message = "Delete the specified package successfully."),
            @ApiResponse(code = 404, message = "The specified package is not existent."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
        }
    )
    @ApiOperation(value = "Delete a package with the package name.")
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
    @ApiOperation(
        value = "Get all the versions of a package.",
        response = String.class,
        responseContainer = "List"
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message = "Return the package versions of the specified package."),
            @ApiResponse(code = 404, message = "The specified package is not existent."),
            @ApiResponse(code = 412, message = "The package name is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
    @ApiOperation(
        value = "Get all the specified type packages in a namespace.",
        response = PackageMetadata.class
    )
    @ApiResponses(
        value = {
            @ApiResponse(code = 200, message =
                "Return all the specified type package names in the specified namespace."),
            @ApiResponse(code = 412, message = "The package type is illegal."),
            @ApiResponse(code = 500, message = "Internal server error.")
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
