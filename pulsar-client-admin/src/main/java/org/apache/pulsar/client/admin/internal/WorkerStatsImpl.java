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
package org.apache.pulsar.client.admin.internal;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.WorkerStats;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.functions.proto.InstanceCommunication.Metrics;
import static org.apache.pulsar.client.admin.internal.FunctionsImpl.mergeJson;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WorkerStatsImpl extends BaseResource implements WorkerStats {

    private final WebTarget workerStats;

    public WorkerStatsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.workerStats = web.path("/admin/worker-stats");
    }

    @Override
    public Metrics getFunctionsStats() throws PulsarAdminException {
        try {
            Response response = request(workerStats.path("functions")).get();
           if (!response.getStatusInfo().equals(Response.Status.OK)) {
               throw new ClientErrorException(response);
           }
           String jsonResponse = response.readEntity(String.class);
           Metrics.Builder metricsBuilder = Metrics.newBuilder();
           mergeJson(jsonResponse, metricsBuilder);
           return metricsBuilder.build();
       } catch (Exception e) {
           throw getApiException(e);
       }
   }
}