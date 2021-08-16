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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.apache.pulsar.common.intercept.InterceptException;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;

/**
 *  Exception handler for handle exception.
 */
public class ExceptionHandler {

    public void handle(ServletResponse response, Exception ex) throws IOException {
        if (ex instanceof InterceptException) {
            String reason = ex.getMessage();
            byte[] content = reason.getBytes(StandardCharsets.UTF_8);
            MetaData.Response info = new MetaData.Response();
            info.setHttpVersion(HttpVersion.HTTP_1_1);
            info.setReason(reason);
            info.setStatus(((InterceptException) ex).getErrorCode());
            info.setContentLength(content.length);
            if (response instanceof org.eclipse.jetty.server.Response) {
                ((org.eclipse.jetty.server.Response) response).getHttpChannel().sendResponse(info,
                        ByteBuffer.wrap(content), true);
            } else {
                ((HttpServletResponse) response).sendError(((InterceptException) ex).getErrorCode(),
                        ex.getMessage());
            }
        } else {
            ((HttpServletResponse) response).sendError(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                    ex.getMessage());
        }
    }
}
