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
package org.apache.pulsar.broker.admin.impl;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConfigFieldDefinition;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.service.api.Sources;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

public class SourcesBase extends AdminResource {

    Sources<? extends WorkerService> sources() {
        return validateAndGetWorkerService().getSources();
    }

    @POST
    @ApiOperation(value = "Creates a new Pulsar Source in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Pulsar Function successfully created"),
            @ApiResponse(code = 400, message =
                    "Invalid request (Function already exists or Tenant, Namespace or Name is not provided, etc.)"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")

    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void registerSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String sourcePkgUrl,
            @ApiParam(value =
                    "You can submit a source (in any languages that you are familiar with) to a Pulsar cluster. "
                            + "Follow the steps below.\n"
                            + "1. Create a JSON object using some of the following parameters.\n"
                            + "A JSON value presenting configuration payload of a Pulsar Source."
                            + " An example of the expected functions can be found here.\n"
                            + "- **classname**\n"
                            + "  The class name of a Pulsar Source if archive is file-url-path (file://).\n"
                            + "- **topicName**\n"
                            + "  The Pulsar topic to which data is sent.\n"
                            + "- **serdeClassName**\n"
                            + "  The SerDe classname for the Pulsar Source.\n"
                            + "- **schemaType**\n"
                            + "  The schema type (either a builtin schema like 'avro', 'json', etc.. or  "
                            + "  custom Schema class name to be used to"
                            + " encode messages emitted from the Pulsar Source\n"
                            + "- **configs**\n"
                            + "  Source config key/values\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName(that is how the secret is going"
                            + " to be accessed in the function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of an value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method. \n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Source"
                            + " (i.e. the number of a Pulsar Source instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (aka delivery semantics) applied to the Pulsar Source.  "
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Source runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **archive**\n"
                            + "  The path to the NAR archive for the Pulsar Source. It also supports url-path "
                            + "  [http/https/file (file protocol assumes that file already exists on worker host)] "
                            + "  from which worker can download the package.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime.\n"
                            + "2. Encapsulate the JSON object to a multipart object.",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.TEXT_PLAIN,
                                    value = " Example \n"
                                            + "\n"
                                            + "1. Create a JSON object. \n"
                                            + "\n"
                                            + "{\n"
                                            + "\t\"tenant\": \"public\",\n"
                                            + "\t\"namespace\": \"default\",\n"
                                            + "\t\"name\": \"pulsar-io-mysql\",\n"
                                            + "\t\"className\": \"TestSourceMysql\",\n"
                                            + "\t\"topicName\": \"pulsar-io-mysql\",\n"
                                            + "\t\"parallelism\": \"1\",\n"
                                            + "\t\"archive\": \"/connectors/pulsar-io-mysql-0.0.1.nar\",\n"
                                            + "\t\"schemaType\": \"avro\"\n"
                                            + "}\n"
                                            + "\n"
                                            + "\n"
                                            + "2. Encapsulate the JSON object to a multipart object (in Python). \n"
                                            + "\n"
                                            + "from requests_toolbelt.multipart.encoder import MultipartEncoder \n"
                                            + "mp_encoder = MultipartEncoder( \n"
                                            + "\t[('sourceConfig', "
                                            + "(None, json.dumps(config), 'application/json'))])\n"
                            )
                    )
            )
            final @FormDataParam("sourceConfig") SourceConfig sourceConfig) {
        sources().registerSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
            sourcePkgUrl, sourceConfig, authParams());
    }

    @PUT
    @ApiOperation(value = "Updates a Pulsar Source currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message =
                    "Invalid request (Function already exists or Tenant, Namespace or Name is not provided, etc.)"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 200, message = "Pulsar Function successfully updated"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public void updateSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName,
            final @FormDataParam("data") InputStream uploadedInputStream,
            final @FormDataParam("data") FormDataContentDisposition fileDetail,
            final @FormDataParam("url") String sourcePkgUrl,
            @ApiParam(
                    value = "A JSON value presenting configuration payload of a Pulsar Source."
                            + " An example of the expected functions can be found here.\n"
                            + "- **classname**\n"
                            + "  The class name of a Pulsar Source if archive is file-url-path (file://).\n"
                            + "- **topicName**\n"
                            + "  The Pulsar topic to which data is sent.\n"
                            + "- **serdeClassName**\n"
                            + "  The SerDe classname for the Pulsar Source.\n"
                            + "- **schemaType**\n"
                            + "  The schema type (either a builtin schema like 'avro', 'json', etc.. or  "
                            + "  custom Schema class name to be used to encode"
                            + " messages emitted from the Pulsar Source\n"
                            + "- **configs**\n"
                            + "  Pulsar Source config key/values\n"
                            + "- **secrets**\n"
                            + "  This is a map of secretName(that is how the secret is going to"
                            + " be accessed in the function via context) to an object that"
                            + "  encapsulates how the secret is fetched by the underlying secrets provider."
                            + " The type of an value here can be found by the"
                            + "  SecretProviderConfigurator.getSecretObjectType() method.\n"
                            + "- **parallelism**\n"
                            + "  The parallelism factor of a Pulsar Source"
                            + " (i.e. the number of a Pulsar Source instances to run).\n"
                            + "- **processingGuarantees**\n"
                            + "  The processing guarantees (aka delivery semantics) applied to the Pulsar Source.  "
                            + "  Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]\n"
                            + "- **resources**\n"
                            + "  The size of the system resources allowed by the Pulsar Source runtime."
                            + " The resources include: cpu, ram, disk.\n"
                            + "- **archive**\n"
                            + "  The path to the NAR archive for the Pulsar Source. It also supports url-path "
                            + "  [http/https/file (file protocol assumes that file already exists on worker host)] "
                            + "  from which worker can download the package.\n"
                            + "- **runtimeFlags**\n"
                            + "  Any flags that you want to pass to the runtime.\n",
                    examples = @Example(
                            value = @ExampleProperty(
                                    mediaType = MediaType.APPLICATION_JSON,
                                    value = "{\n"
                                            + "  \"tenant\": public\n"
                                            + "  \"namespace\": default\n"
                                            + "  \"name\": pulsar-io-mysql\n"
                                            + "  \"className\": TestSourceMysql\n"
                                            + "  \"topicName\": pulsar-io-mysql\n"
                                            + "  \"parallelism\": 1\n"
                                            + "  \"archive\": /connectors/pulsar-io-mysql-0.0.1.nar\n"
                                            + "  \"schemaType\": avro\n"
                                            + "}\n"
                            )
                    )
            )
            final @FormDataParam("sourceConfig") SourceConfig sourceConfig,
            @ApiParam(value = "Update options for Pulsar Source")
            final @FormDataParam("updateOptions") UpdateOptionsImpl updateOptions) {
        sources().updateSource(tenant, namespace, sourceName, uploadedInputStream, fileDetail,
            sourcePkgUrl, sourceConfig, authParams(), updateOptions);
    }


    @DELETE
    @ApiOperation(value = "Deletes a Pulsar Source currently running in cluster mode")
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 200, message = "The function was successfully deleted"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    public void deregisterSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().deregisterFunction(tenant, namespace, sourceName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Fetches information about a Pulsar Source currently running in cluster mode",
            response = SourceConfig.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}")
    public SourceConfig getSourceInfo(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) throws IOException {
        return sources().getSourceInfo(tenant, namespace, sourceName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Source instance",
            response = SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/status")
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceInstanceStatus(
            @ApiParam(value = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @ApiParam(value = "The instanceId of a Pulsar Source"
                    + " (if instance-id is not provided, the stats of all instances is returned).") final @PathParam(
                    "instanceId") String instanceId) throws IOException {
        return sources().getSourceInstanceStatus(
            tenant, namespace, sourceName, instanceId, uri.getRequestUri(), authParams());
    }

    @GET
    @ApiOperation(
            value = "Displays the status of a Pulsar Source running in cluster mode",
            response = SourceStatus.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}/{sourceName}/status")
    public SourceStatus getSourceStatus(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) throws IOException {
        return sources().getSourceStatus(tenant, namespace, sourceName, uri.getRequestUri(), authParams());
    }

    @GET
    @ApiOperation(
            value = "Lists all Pulsar Sources currently deployed in a given namespace",
            response = String.class,
            responseContainer = "Collection"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 500, message = "Internal Server Error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{tenant}/{namespace}")
    public List<String> listSources(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace) {
        return sources().listFunctions(tenant, namespace, authParams());
    }

    @POST
    @ApiOperation(value = "Restart an instance of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this source"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(
            @ApiParam(value = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @ApiParam(value = "The instanceId of a Pulsar Source"
                    + " (if instance-id is not provided, the stats of all instances is returned).") final @PathParam(
                    "instanceId") String instanceId) {
        sources().restartFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Restart all instances of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    public void restartSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().restartFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @ApiOperation(value = "Stop instance of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(
            @ApiParam(value = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @ApiParam(value = "The instanceId of a Pulsar Source (if instance-id is not provided,"
                    + " the stats of all instances is returned).") final @PathParam("instanceId") String instanceId) {
        sources().stopFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Stop all instances of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/stop")
    @Consumes(MediaType.APPLICATION_JSON)
    public void stopSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().stopFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @POST
    @ApiOperation(value = "Start an instance of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/{instanceId}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(
            @ApiParam(value = "The tenant of a Pulsar Source") final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source") final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source") final @PathParam("sourceName") String sourceName,
            @ApiParam(value = "The instanceId of a Pulsar Source (if instance-id is not provided,"
                    + " the stats of all instances is returned).") final @PathParam("instanceId") String instanceId) {
        sources().startFunctionInstance(tenant, namespace, sourceName, instanceId,
                uri.getRequestUri(), authParams());
    }

    @POST
    @ApiOperation(value = "Start all instances of a Pulsar Source", response = Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 401, message = "Client is not authorized to perform operation"),
            @ApiResponse(code = 404, message = "Not Found(The Pulsar Source doesn't exist)"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Path("/{tenant}/{namespace}/{sourceName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public void startSource(
            @ApiParam(value = "The tenant of a Pulsar Source")
            final @PathParam("tenant") String tenant,
            @ApiParam(value = "The namespace of a Pulsar Source")
            final @PathParam("namespace") String namespace,
            @ApiParam(value = "The name of a Pulsar Source")
            final @PathParam("sourceName") String sourceName) {
        sources().startFunctionInstances(tenant, namespace, sourceName, authParams());
    }

    @GET
    @ApiOperation(
            value = "Fetches the list of built-in Pulsar IO sources",
            response = ConnectorDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 400, message = "Invalid request"),
            @ApiResponse(code = 408, message = "Request timeout"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources")
    public List<ConnectorDefinition> getSourceList() {
        return sources().getSourceList();
    }

    @GET
    @ApiOperation(
            value = "Fetches information about config fields associated with the specified builtin source",
            response = ConfigFieldDefinition.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "The requester doesn't have admin permissions"),
            @ApiResponse(code = 404, message = "builtin source does not exist"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later.")
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/builtinsources/{name}/configdefinition")
    public List<ConfigFieldDefinition> getSourceConfigDefinition(
            @ApiParam(value = "The name of the builtin source")
            final @PathParam("name") String name) throws IOException {
        return sources().getSourceConfigDefinition(name);
    }

    @POST
    @ApiOperation(
            value = "Reload the built-in connectors, including Sources and Sinks",
            response = Void.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "This operation requires super-user access"),
            @ApiResponse(code = 503, message = "Function worker service is now initializing. Please try again later."),
            @ApiResponse(code = 500, message = "Internal server error")
    })
    @Path("/reloadBuiltInSources")
    public void reloadSources() {
        sources().reloadConnectors(authParams());
    }
}
