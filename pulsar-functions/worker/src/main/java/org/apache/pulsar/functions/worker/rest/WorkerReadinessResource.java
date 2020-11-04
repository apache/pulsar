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
package org.apache.pulsar.functions.worker.rest;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.pulsar.functions.worker.WorkerService;

import java.util.function.Supplier;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/")
public class WorkerReadinessResource implements Supplier<WorkerService>  {

  public static final String ATTRIBUTE_WORKER_SERVICE = "worker";

  private WorkerService workerService;
  @Context
  protected ServletContext servletContext;
  @Context
  protected HttpServletRequest httpRequest;

  @Override
  public synchronized WorkerService get() {
    if (this.workerService == null) {
      this.workerService = (WorkerService) servletContext.getAttribute(ATTRIBUTE_WORKER_SERVICE);
    }
    return this.workerService;
  }

  @GET
  @ApiOperation(
    value = "Determines whether the worker service is initialized and ready for use",
    response = Boolean.class
  )
  @ApiResponses(value = {
    @ApiResponse(code = 400, message = "Invalid request"),
    @ApiResponse(code = 408, message = "Request timeout")
  })
  @Path("/initialized")
  public boolean isInitialized() {
    if (!get().isInitialized()) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return true;
  }
}
