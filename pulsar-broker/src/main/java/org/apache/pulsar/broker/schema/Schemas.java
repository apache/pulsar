package org.apache.pulsar.broker.schema;

import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.common.schema.Schema;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/schemas")
public class Schemas extends PulsarWebResource {

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
            .thenAccept(response::resume);
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
        schemaRegistryService.deleteSchema(schemaId).thenAccept(ignore ->
            response.resume(Response.ok().build())
        );
    }

    @POST @Path("/{property}/{namespace}/{topic}.json")
    @Produces(MediaType.APPLICATION_JSON)
    public void putSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse response
    ) {
        validateAdminAccessOnProperty(property);
        String schemaId = property + "/" + namespace + "/" + topic;
        Schema schema = Schema.newBuilder().id(schemaId).build();
        schemaRegistryService.putSchema(schema)
            .thenAccept(response::resume);
    }

}
