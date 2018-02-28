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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.isNull;
import static org.apache.pulsar.common.util.Codec.decode;

import com.google.common.annotations.VisibleForTesting;
import io.swagger.annotations.ApiOperation;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.schema.SchemaVersion;

@Path("/schemas")
public class SchemasResource extends AdminResource {

    private final Clock clock;

    @VisibleForTesting
    SchemasResource(Clock clock) {
        super();
        this.clock = clock;
    }

    public SchemasResource() {
        this(Clock.systemUTC());
    }

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
        pulsar().getSchemaRegistryService().getSchema(schemaId)
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
        @PathParam("version") @Encoded String version,
        @Suspended final AsyncResponse response
    ) {
        validateDestinationAndAdminOperation(property, cluster, namespace, topic);

        String schemaId = buildSchemaId(property, cluster, namespace, topic);
        SchemaVersion v = pulsar().getSchemaRegistryService().versionFromBytes(version.getBytes());
        pulsar().getSchemaRegistryService().getSchema(schemaId, v)
            .handle((schema, error) -> {
                if (isNull(error)) {
                    response.resume(
                        Response.ok()
                            .encoding(MediaType.APPLICATION_JSON)
                            .entity(new GetSchemaResponse(
                                schema.schema, schema.version)
                            ).build()
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
        pulsar().getSchemaRegistryService().deleteSchema(schemaId, clientAppId())
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

        pulsar().getSchemaRegistryService().putSchema(
            buildSchemaId(property, cluster, namespace, topic),
            Schema.newBuilder()
                .data(payload.schema.getBytes())
                .isDeleted(false)
                .timestamp(clock.millis())
                .type(SchemaType.valueOf(payload.type))
                .user(clientAppId())
                .properties(payload.properties)
                .build()
        ).thenAccept(version ->
            response.resume(
                Response.accepted().entity(
                    new PostSchemaResponse(version)
                ).build()
            )
        );
    }

    private String buildSchemaId(String property, String cluster, String namespace, String topic) {
        return property + "_" + cluster + "_" + namespace + "_" + topic;
    }

    private void validateDestinationAndAdminOperation(String property, String cluster, String namespace, String topic) {
        TopicName destinationName = TopicName.get(
            domain(), property, cluster, namespace, decode(topic)
        );

        try {
            validateDestinationExists(destinationName);
            validateAdminAccessOnProperty(destinationName.getProperty());
            validateTopicOwnership(destinationName, false);
        } catch (RestException e) {
            if (e.getResponse().getStatus() == Response.Status.UNAUTHORIZED.getStatusCode()) {
                throw new RestException(Response.Status.NOT_FOUND, "Not Found");
            } else {
                throw e;
            }
        }
    }

    private void validateDestinationExists(TopicName dn) {
        try {
            Topic topic = pulsar().getBrokerService().getTopicReference(dn.toString());
            checkNotNull(topic);
        } catch (Exception e) {
            throw new RestException(Response.Status.NOT_FOUND, "Topic not found");
        }
    }

    static class GetSchemaResponse {
        public final SchemaVersion version;
        public final Schema schema;

        GetSchemaResponse(Schema schema, SchemaVersion version) {
            this.schema = schema;
            this.version = version;
        }
    }

    static class PostSchemaPayload {
        public final String type;
        public final String schema;
        public final Map<String, String> properties;

        @VisibleForTesting
        PostSchemaPayload(String type, String schema, Map<String, String> properties) {
            this.type = type;
            this.schema = schema;
            this.properties = properties;
        }
    }

    static class PostSchemaResponse {
        public SchemaVersion version;

        PostSchemaResponse(SchemaVersion version) {
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PostSchemaResponse that = (PostSchemaResponse) o;
            return version == that.version;
        }

        @Override
        public int hashCode() {

            return Objects.hash(version);
        }
    }

    static class DeleteSchemaResponse {
        public SchemaVersion version;

        DeleteSchemaResponse(SchemaVersion version) {
            this.version = version;
        }
    }

}
