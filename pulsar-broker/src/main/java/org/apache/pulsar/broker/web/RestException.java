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
package org.apache.pulsar.broker.web;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ErrorData;

/**
 * Exception used to provide better error messages to clients of the REST API.
 */
@SuppressWarnings("serial")
public class RestException extends WebApplicationException {
    private Throwable cause = null;
    static String getExceptionData(Throwable t) {
        StringWriter writer = new StringWriter();
        writer.append("\n --- An unexpected error occurred in the server ---\n\n");
        if (t != null) {
            writer.append("Message: ").append(t.getMessage()).append("\n\n");
        }
        writer.append("Stacktrace:\n\n");

        t.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    public RestException(Response.Status status, String message) {
        this(status.getStatusCode(), message);
    }

    public RestException(int code, String message) {
        super(message, Response.status(code).entity(new ErrorData(message)).type(MediaType.APPLICATION_JSON).build());
    }

    public RestException(Throwable t) {
        super(getResponse(t));
    }

    public RestException(Response.Status status, Throwable t) {
        this(status.getStatusCode(), t.getMessage());
        this.cause = t;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    public RestException(PulsarAdminException cae) {
        this(cae.getStatusCode(), cae.getHttpError());
    }

    private static Response getResponse(Throwable t) {
        if (t instanceof WebApplicationException) {
            WebApplicationException e = (WebApplicationException) t;
            return e.getResponse();
        } else {
            return Response
                .status(Status.INTERNAL_SERVER_ERROR)
                .entity(getExceptionData(t))
                .type(MediaType.TEXT_PLAIN)
                .build();
        }
    }
}
