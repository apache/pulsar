package org.apache.pulsar.broker.schema;

import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Clock;

import static java.util.Objects.isNull;

@Path("/schemas")
public class SchemasResource extends PulsarWebResource {

    private final Clock clock = Clock.systemUTC();

    private final SchemaRegistryService schemaRegistryService =
        pulsar().getSchemaRegistryService();

    @GET @Path("/{property}/{namespace}/{topic}.json")
    @Produces(MediaType.APPLICATION_JSON)
    public void getSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("version") long version,
        @Suspended final AsyncResponse response
    ) {
        validateAdminAccessOnProperty(property);
        String schemaId = property + "/" + namespace + "/" + topic;
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

    @DELETE @Path("/{property}/{namespace}/{topic}.json")
    @Produces(MediaType.APPLICATION_JSON)
    public void deleteSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateAdminAccessOnProperty(property);
        String schemaId = property + "/" + namespace + "/" + topic;
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

    @POST @Path("/{property}/{namespace}/{topic}.json")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public void postSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        PostSchemaPayload payload,
        @Suspended final AsyncResponse response
    ) {
        validateAdminAccessOnProperty(property);
        schemaRegistryService.putSchema(
            Schema.newBuilder()
                .id(property + "/" + namespace + "/" + topic)
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
