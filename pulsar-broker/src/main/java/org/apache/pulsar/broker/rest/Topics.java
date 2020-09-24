package org.apache.pulsar.broker.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.pulsar.common.policies.data.ProduceMessageRequest;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

@Path("/topics")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/topics", description = "Apis for produce,consume and ack message on topics.", tags = "topics")
public class Topics extends TopicBase {

    @POST
    @Path("/{tenant}/{namespace}/{topic}")
    @ApiOperation(value = "Produce message to a topic.", response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "tenant/namespace/topic doesn't exit"),
            @ApiResponse(code = 412, message = "Namespace name is not valid"),
            @ApiResponse(code = 500, message = "Internal server error") })
    public void produceOnTopic(@Suspended final AsyncResponse asyncResponse,
                               @ApiParam(value = "Specify the tenant", required = true)
                               @PathParam("tenant") String tenant,
                               @ApiParam(value = "Specify the namespace", required = true)
                               @PathParam("namespace") String namespace,
                               @ApiParam(value = "Specify topic name", required = true)
                               @PathParam("topic") @Encoded String encodedTopic,
                               @QueryParam("authoritative") @DefaultValue("false") boolean authoritative,
                               @QueryParam("producerName") @DefaultValue("RestProducer") String producerName,
                               ProduceMessageRequest produceMessageRequest) {
        validateTopicName(tenant, namespace, encodedTopic);
        internalPublishMessage(asyncResponse, produceMessageRequest, authoritative, producerName);
    }

    public void produceOnTopicPartition() {

    }

}
