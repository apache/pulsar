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

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Worker;
import org.apache.pulsar.client.api.Authentication;
//import org.apache.pulsar.functions.proto.InstanceCommunication.Metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.WorkerInfo;

@Slf4j
public class WorkerImpl extends BaseResource implements Worker {

    private final WebTarget workerStats;
    private final WebTarget worker;

    public WorkerImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.worker = web.path("/admin/v2/worker");
        this.workerStats = web.path("/admin/v2/worker-stats");
    }

//    @Override
//    public Metrics getFunctionsStats() throws PulsarAdminException {
//        try {
//            Response response = request(workerStats.path("functionsmetrics")).get();
//           if (!response.getStatusInfo().equals(Response.Status.OK)) {
//               throw new ClientErrorException(response);
//           }
//           String jsonResponse = response.readEntity(String.class);
//           Metrics.Builder metricsBuilder = Metrics.newBuilder();
//           mergeJson(jsonResponse, metricsBuilder);
//           return metricsBuilder.build();
//       } catch (Exception e) {
//           throw getApiException(e);
//       }
//   }

    @Override
    public Collection<org.apache.pulsar.common.stats.Metrics> getMetrics() throws PulsarAdminException {
        try {
            return request(workerStats.path("metrics"))
                    .get(new GenericType<List<org.apache.pulsar.common.stats.Metrics>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<WorkerInfo> getCluster() throws PulsarAdminException {
        try {
            return request(worker.path("cluster"))
                    .get(new GenericType<List<WorkerInfo>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public WorkerInfo getClusterLeader() throws PulsarAdminException {
        try {
            return request(worker.path("cluster").path("leader"))
                    .get(new GenericType<WorkerInfo>(){});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, Collection<String>> getAssignments() throws PulsarAdminException {
        try {
            Response response = request(worker.path("assignments")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            String jsonResponse = response.readEntity(String.class);
            Type type = new TypeToken<Map<String, Collection<String>>>(){}.getType();
            Map<String, Collection<String>> assignments = new Gson().fromJson(jsonResponse, type);
            return assignments;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}