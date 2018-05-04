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
package org.apache.pulsar.functions.worker.rest.api.v2;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;

@Slf4j
@Path("/connectors")
public class ConnectorApiV2Resource extends FunctionApiResource {

    @POST
    @Path("/source/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerSource(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName,
                                      final @FormDataParam("data") InputStream uploadedInputStream,
                                      final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                      final @FormDataParam("functionDetails") String functionDetailsJson) {

        return functions.registerSource(
                tenant, namespace, functionName, uploadedInputStream, fileDetail, functionDetailsJson);
    }

    @POST
    @Path("/sink/{tenant}/{namespace}/{functionName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response registerSink(final @PathParam("tenant") String tenant,
                                      final @PathParam("namespace") String namespace,
                                      final @PathParam("functionName") String functionName,
                                      final @FormDataParam("data") InputStream uploadedInputStream,
                                      final @FormDataParam("data") FormDataContentDisposition fileDetail,
                                      final @FormDataParam("functionDetails") String functionDetailsJson) {

        return functions.registerSink(
                tenant, namespace, functionName, uploadedInputStream, fileDetail, functionDetailsJson);
    }

    @DELETE
    @Path("/{tenant}/{namespace}/{functionName}")
    public Response deregisterConnector(final @PathParam("tenant") String tenant,
                                       final @PathParam("namespace") String namespace,
                                       final @PathParam("functionName") String functionName) {
        return functions.deregisterFunction(
                tenant, namespace, functionName);
    }
}
