/*
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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.admin.impl.PersistentTopicsBase;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for preventing docs conflict before we find a good way to fix
 * <a href="https://github.com/apache/pulsar/issues/18947">ISSUE-18947</a>.
 */
@Path("/persistent")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/persistent", description = "Persistent topic admin apis", tags = "persistent topic")
public class ExtPersistentTopics extends PersistentTopicsBase {

    @PUT
    @Consumes(PartitionedTopicMetadata.MEDIA_TYPE)
    @Path("/{tenant}/{namespace}/{topic}/partitions")
    @ApiOperation(value = "Create a partitioned topic.",
            notes = "It needs to be called before creating a producer on a partitioned topic.")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Current broker doesn't serve the namespace of this topic"),
            @ApiResponse(code = 401, message = "Don't have permission to administrate resources on this tenant"),
            @ApiResponse(code = 403, message = "Don't have admin permission"),
            @ApiResponse(code = 404, message = "Tenant or namespace doesn't exist"),
            @ApiResponse(code = 406, message = "The number of partitions should be more than 0 and"
                    + " less than or equal to maxNumPartitionsPerPartitionedTopic"),
            @ApiResponse(code = 409, message = "Partitioned topic already exist"),
            @ApiResponse(code = 412,
                    message = "Failed Reason : Name is invalid or Namespace does not have any clusters configured"),
            @ApiResponse(code = 500, message = "Internal server error"),
            @ApiResponse(code = 503, message = "Failed to validate global cluster configuration")
    })
    public void createPartitionedTopic(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Specify the tenant", required = true)
            @PathParam("tenant") String tenant,
            @ApiParam(value = "Specify the namespace", required = true)
            @PathParam("namespace") String namespace,
            @ApiParam(value = "Specify topic name", required = true)
            @PathParam("topic") @Encoded String encodedTopic,
            @ApiParam(value = "The metadata for the topic",
                    required = true, type = "PartitionedTopicMetadata") PartitionedTopicMetadata metadata,
            @QueryParam("createLocalTopicOnly") @DefaultValue("false") boolean createLocalTopicOnly) {
        try {
            validateNamespaceName(tenant, namespace);
            validateGlobalNamespaceOwnership();
            validatePartitionedTopicName(tenant, namespace, encodedTopic);
            validateTopicPolicyOperation(topicName, PolicyName.PARTITION, PolicyOperation.WRITE);
            validateCreateTopic(topicName);
            internalCreatePartitionedTopic(asyncResponse, metadata.partitions, createLocalTopicOnly,
                    metadata.properties);
        } catch (Exception e) {
            log.error("[{}] Failed to create partitioned topic {}", clientAppId(), topicName, e);
            resumeAsyncResponseExceptionally(asyncResponse, e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentTopics.class);
}
