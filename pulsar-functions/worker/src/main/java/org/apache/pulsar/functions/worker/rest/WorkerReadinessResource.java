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
package org.apache.pulsar.functions.worker.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import java.util.function.Supplier;
import org.apache.pulsar.functions.worker.WorkerService;

@Path("/")
public class WorkerReadinessResource implements Supplier<WorkerService> {

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
  @Operation(
    summary = "Determines whether the worker service is initialized and ready for use"
  )
  @ApiResponses(value = {
    @ApiResponse(responseCode = "200",
      description = "Determines whether the worker service is initialized and ready for use",
      content = @Content(schema = @Schema(implementation = Boolean.class))),
    @ApiResponse(responseCode = "400", description = "Invalid request"),
    @ApiResponse(responseCode = "408", description = "Request timeout")
  })
  @Path("/initialized")
  public boolean isInitialized() {
    if (!get().isInitialized()) {
      throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
    }
    return true;
  }
}
