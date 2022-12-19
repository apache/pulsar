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
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http.MetaData;

/**
 *  Exception handler for handle exception.
 */
public class ExceptionHandler {

    public void handle(ServletResponse response, Exception ex) throws IOException {
        if (ex instanceof InterceptException) {
            if (response instanceof org.eclipse.jetty.server.Response) {
                String errorData = ObjectMapperFactory
                        .getThreadLocal().writeValueAsString(new ErrorData(ex.getMessage()));
                byte[] errorBytes = errorData.getBytes(StandardCharsets.UTF_8);
                int errorCode = ((InterceptException) ex).getErrorCode();
                HttpFields httpFields = new HttpFields();
                HttpField httpField = new HttpField(HttpHeader.CONTENT_TYPE, "application/json;charset=utf-8");
                httpFields.add(httpField);
                MetaData.Response info = new MetaData.Response(HttpVersion.HTTP_1_1, errorCode, httpFields);
                info.setHttpVersion(HttpVersion.HTTP_1_1);
                info.setReason(errorData);
                info.setStatus(errorCode);
                info.setContentLength(errorBytes.length);
                ((org.eclipse.jetty.server.Response) response).getHttpChannel().sendResponse(info,
                        ByteBuffer.wrap(errorBytes),
                        true);
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
