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
package org.apache.pulsar.common.configuration;

import java.io.File;
import java.util.function.Supplier;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;

/**
 * Web resource used by the VIP service to check to availability of the service instance.
 */
@Path("/status.html")
public class VipStatus {

    public static final String ATTRIBUTE_STATUS_FILE_PATH = "statusFilePath";
    public static final String ATTRIBUTE_IS_READY_PROBE = "isReadyProbe";

    @Context
    protected ServletContext servletContext;

    @GET
    @Context
    public String checkStatus() {
        String statusFilePath = (String) servletContext.getAttribute(ATTRIBUTE_STATUS_FILE_PATH);
        @SuppressWarnings("unchecked")
        Supplier<Boolean> isReadyProbe = (Supplier<Boolean>) servletContext.getAttribute(ATTRIBUTE_IS_READY_PROBE);

        boolean isReady = isReadyProbe != null ? isReadyProbe.get() : true;

        if (statusFilePath != null) {
            File statusFile = new File(statusFilePath);
            if (isReady && statusFile.exists() && statusFile.isFile()) {
                return "OK";
            }
        }
        throw new WebApplicationException(Status.NOT_FOUND);
    }

}
