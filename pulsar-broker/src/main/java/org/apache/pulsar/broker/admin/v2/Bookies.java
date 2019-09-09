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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.util.Map.Entry;
import java.util.Optional;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/bookies")
@Api(value = "/bookies", description = "Configure bookies rack placement", tags = "bookies")
@Produces(MediaType.APPLICATION_JSON)
public class Bookies extends AdminResource {

    @GET
    @Path("/racks-info")
    @ApiOperation(value = "Gets the rack placement information for all the bookies in the cluster", response = BookiesRackConfiguration.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public BookiesRackConfiguration getBookiesRackInfo() throws Exception {
        validateSuperUserAccess();

        return localZkCache().getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                new Deserializer<BookiesRackConfiguration>() {

                    @Override
                    public BookiesRackConfiguration deserialize(String key, byte[] content) throws Exception {
                        return ObjectMapperFactory.getThreadLocal().readValue(content, BookiesRackConfiguration.class);
                    }

                }).orElse(new BookiesRackConfiguration());
    }

    @GET
    @Path("/racks-info/{bookie}")
    @ApiOperation(value = "Gets the rack placement information for a specific bookie in the cluster", response = BookieInfo.class)
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public BookieInfo getBookieRackInfo(@PathParam("bookie") String bookieAddress) throws Exception {
        validateSuperUserAccess();

        BookiesRackConfiguration racks = localZkCache()
                .getData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, (key, content) -> ObjectMapperFactory
                        .getThreadLocal().readValue(content, BookiesRackConfiguration.class))
                .orElse(new BookiesRackConfiguration());

        return racks.getBookie(bookieAddress)
                .orElseThrow(() -> new RestException(Status.NOT_FOUND, "Bookie address not found: " + bookieAddress));
    }

    @DELETE
    @Path("/racks-info/{bookie}")
    @ApiOperation(value = "Removed the rack placement information for a specific bookie in the cluster")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void deleteBookieRackInfo(@PathParam("bookie") String bookieAddress) throws Exception {
        validateSuperUserAccess();


        Optional<Entry<BookiesRackConfiguration, Stat>> entry = localZkCache()
            .getEntry(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, (key, content) -> ObjectMapperFactory
                .getThreadLocal().readValue(content, BookiesRackConfiguration.class));

        if (entry.isPresent()) {
            BookiesRackConfiguration racks = entry.get().getKey();
            if (!racks.removeBookie(bookieAddress)) {
                throw new RestException(Status.NOT_FOUND, "Bookie address not found: " + bookieAddress);
            } else {
                localZk().setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH,
                    jsonMapper().writeValueAsBytes(racks),
                    entry.get().getValue().getVersion());
                log.info("Removed {} from rack mapping info", bookieAddress);
            }
        } else {
            throw new RestException(Status.NOT_FOUND, "Bookie rack placement info is not found");
        }

    }

    @POST
    @Path("/racks-info/{bookie}")
    @ApiOperation(value = "Updates the rack placement information for a specific bookie in the cluster")
    @ApiResponses(value = { @ApiResponse(code = 403, message = "Don't have admin permission") })
    public void updateBookieRackInfo(@PathParam("bookie") String bookieAddress, @QueryParam("group") String group,
            BookieInfo bookieInfo) throws Exception {
        validateSuperUserAccess();

        if (group == null) {
            throw new RestException(Status.PRECONDITION_FAILED, "Bookie 'group' parameters is missing");
        }

        Optional<Entry<BookiesRackConfiguration, Stat>> entry = localZkCache()
                .getEntry(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, (key, content) -> ObjectMapperFactory
                        .getThreadLocal().readValue(content, BookiesRackConfiguration.class));

        if (entry.isPresent()) {
            // Update the racks info
            BookiesRackConfiguration racks = entry.get().getKey();
            racks.updateBookie(group, bookieAddress, bookieInfo);

            localZk().setData(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper().writeValueAsBytes(racks),
                    entry.get().getValue().getVersion());
            log.info("Updated rack mapping info for {}", bookieAddress);
        } else {
            // Creates the z-node with racks info
            BookiesRackConfiguration racks = new BookiesRackConfiguration();
            racks.updateBookie(group, bookieAddress, bookieInfo);
            zkCreate(ZkBookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper().writeValueAsBytes(racks));
            log.info("Created rack mapping info and added {}", bookieAddress);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Bookies.class);
}
