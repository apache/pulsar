package org.apache.pulsar.broker.schema;

import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.web.PulsarWebResource;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

@Path("/schemas")
public class Schemas extends PulsarWebResource {

    private final SchemaRegistryService schemaRegistryService =
        pulsar().getSchemaRegistryService();

    @GET
    @Path("/{property}/{namespace}/{topic}.json")
    @Produces("text/json")
    public void getSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @QueryParam("version") long version,
        @Suspended final AsyncResponse asyncResponse
    ) {
        String schemaId = property + "/" + namespace + "/" + topic;
        schemaRegistryService.getSchema(schemaId).thenAccept(schema -> {

        });
    }

    @DELETE
    @Path("/{property}/{namespace}/{topic}.json")
    @Produces("text/json")
    public void deleteSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse asyncResponse
    ) {

    }

    @PUT
    @Path("/{property}/{namespace}/{topic}.json")
    @Produces("text/json")
    public void putSchema(
        @PathParam("property") String property,
        @PathParam("namespace") String namespace,
        @PathParam("topic") String topic,
        @Suspended final AsyncResponse asyncResponse
    ) {

    }

}
