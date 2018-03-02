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
package org.apache.pulsar.broker.admin.v2;

import static com.google.common.base.Preconditions.checkNotNull;


import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 */
@Path("/non-persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/non-persistent", description = "Non-Persistent topic admin apis", tags = "non-persistent topic")
public class NonPersistentTopics extends PersistentTopics {
    private static final Logger log = LoggerFactory.getLogger(NonPersistentTopics.class);

    @GET
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Get partitioned topic metadata.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public PartitionedTopicMetadata getPartitionedMetadata(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        return getPartitionedTopicMetadata(topicName, authoritative);
    }

    @GET
    @Path("{property}/{namespace}/{topic}/stats")
    @ApiOperation(value = "Get the stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public NonPersistentTopicStats getStats(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return ((NonPersistentTopic) topic).getStats();
    }

    @GET
    @Path("{property}/{namespace}/{topic}/internalStats")
    @ApiOperation(value = "Get the internal stats for the topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public PersistentTopicInternalStats getInternalStats(@PathParam("property") String property,
            @PathParam("namespace") String namespace, @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        validateAdminOperationOnTopic(topicName, authoritative);
        Topic topic = getTopicReference(topicName);
        return topic.getInternalStats();
    }

    @PUT
    @Path("/{property}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.", notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist") })
    public void createPartitionedTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic, int numPartitions,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        validateAdminAccessOnProperty(topicName.getProperty());
        if (numPartitions <= 1) {
            throw new RestException(Status.NOT_ACCEPTABLE, "Number of partitions should be more than 1");
        }
        try {
            String path = path(PARTITIONED_TOPIC_PATH_ZNODE, namespaceName.toString(), domain(),
                    topicName.getEncodedLocalName());
            byte[] data = jsonMapper().writeValueAsBytes(new PartitionedTopicMetadata(numPartitions));
            zkCreateOptimistic(path, data);
            // we wait for the data to be synced in all quorums and the observers
            Thread.sleep(PARTITIONED_TOPIC_WAIT_SYNC_TIME_MS);
            log.info("[{}] Successfully created partitioned topic {}", clientAppId(), topicName);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("[{}] Failed to create already existing partitioned topic {}", clientAppId(), topicName);
            throw new RestException(Status.CONFLICT, "Partitioned topic already exist");
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            throw new RestException(e);
        }
    }

    @PUT
    @Path("/{property}/{namespace}/{topic}/unload")
    @ApiOperation(value = "Unload a topic")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Topic does not exist") })
    public void unloadTopic(@PathParam("property") String property, @PathParam("namespace") String namespace,
            @PathParam("topic") @Encoded String encodedTopic,
            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        validateTopicName(property, namespace, encodedTopic);
        log.info("[{}] Unloading topic {}", clientAppId(), topicName);
        if (topicName.isGlobal()) {
            validateGlobalNamespaceOwnership(namespaceName);
        }
        unloadTopic(topicName, authoritative);
    }

    protected void validateAdminOperationOnTopic(TopicName topicName, boolean authoritative) {
        validateAdminAccessOnProperty(topicName.getProperty());
        validateTopicOwnership(topicName, authoritative);
    }

    private Topic getTopicReference(TopicName topicName) {
        try {
            Topic topic = pulsar().getBrokerService().getTopicReference(topicName.toString());
            checkNotNull(topic);
            return topic;
        } catch (Exception e) {
            throw new RestException(Status.NOT_FOUND, "Topic not found");
        }
    }
}
