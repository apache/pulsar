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
package org.apache.pulsar.broker.admin;

import io.swagger.annotations.ApiOperation;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Clock;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.isNull;
import static org.apache.pulsar.common.util.Codec.decode;

@Path("/schemas")
public class SchemasResource extends AdminResource {

    private final Clock clock = Clock.systemUTC();

    private final SchemaRegistryService schemaRegistryService =
        pulsar().getSchemaRegistryService();

    private final BrokerService brokerService =
        pulsar().getBrokerService();

    @GET @Path("/{property}/{cluster}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get topic schema")
    public void getSchema(
        @PathParam("property") String property,
        @PathParam("cluster") String cluster,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(property, cluster, namespace, topic);

        String schemaId = buildSchemaId(property, cluster, namespace, topic);
        schemaRegistryService.getSchema(schemaId)
            .handle((schema, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok()
                            .encoding(MediaType.APPLICATION_JSON)
                            .entity(schema)
                            .build()
                    );
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @GET @Path("/{property}/{cluster}/{namespace}/{topic}/schema/{version}")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get topic schema")
    public void getSchema(
        @PathParam("property") String property,
        @PathParam("cluster") String cluster,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @PathParam("version") long version,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(property, cluster, namespace, topic);

        String schemaId = buildSchemaId(property, cluster, namespace, topic);
        schemaRegistryService.getSchema(schemaId, version)
            .handle((schema, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok()
                            .encoding(MediaType.APPLICATION_JSON)
                            .entity(schema)
                            .build()
                    );
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @DELETE @Path("/{property}/{cluster}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete topic schema")
    public void deleteSchema(
        @PathParam("property") String property,
        @PathParam("cluster") String cluster,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(property, cluster, namespace, topic);

        String schemaId = buildSchemaId(property, cluster, namespace, topic);
        schemaRegistryService.deleteSchema(schemaId, clientAppId())
            .handle((version, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok().entity(
                            new DeleteSchemaResponse(version)
                        ).build()
                    );
                } else {
                    response.resume(error);
                }
                return null;
            });
    }

    @POST @Path("/{property}/{cluster}/{namespace}/{topic}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Post topic schema")
    public void postSchema(
        @PathParam("property") String property,
        @PathParam("cluster") String cluster,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        PostSchemaPayload payload,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(property, cluster, namespace, topic);

        schemaRegistryService.putSchema(
            buildSchemaId(property, cluster, namespace, topic),
            Schema.newBuilder()
                .data(payload.schema.getBytes())
                .isDeleted(false)
                .timestamp(clock.millis())
                .type(SchemaType.valueOf(payload.type))
                .user(clientAppId())
                .build()
        ).thenAccept(version ->
            response.resume(
                Response.ok().entity(
                    new PostSchemaResponse(version)
                ).build()
            )
        );
    }

    private String buildSchemaId(String property, String cluster, String namespace, String topic) {
        return property + "/" + cluster + "/" + namespace + "/" + topic;
    }

    private void validateDestinationAndAdminOperation(String property, String cluster, String namespace, String topic) {
        DestinationName destinationName = DestinationName.get(
            domain(), property, cluster, namespace, decode(topic)
        );

        validateDestinationExists(destinationName);
        validateAdminAccessOnProperty(destinationName.getProperty());
        validateDestinationOwnership(destinationName, false);
    }

    private void validateDestinationExists(DestinationName dn) {
        try {
            Topic topic = brokerService.getTopicReference(dn.toString());
            checkNotNull(topic);
        } catch (Exception e) {
            throw new RestException(Response.Status.NOT_FOUND, "Topic not found");
        }
    }

    class PostSchemaPayload {
        public String type;
        public String schema;
    }

    class PostSchemaResponse {
        public long version;

        PostSchemaResponse(long version) {
            this.version = version;
        }
    }

    class DeleteSchemaResponse {
        public long version;

        DeleteSchemaResponse(long version) {
            this.version = version;
        }
    }

}
