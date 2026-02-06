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
package org.apache.pulsar.broker.web;

import java.io.IOException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 *  Exception handler for handle exception.
 */
@Slf4j
public class ExceptionHandler {

    public void handle(ServletResponse response, Exception ex) throws IOException {
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;
        if (ex instanceof InterceptException) {
            byte[] errorBytes = ObjectMapperFactory
                    .getMapper().writer().writeValueAsBytes(new ErrorData(ex.getMessage()));
            int errorCode = ((InterceptException) ex).getErrorCode();
            httpServletResponse.setStatus(errorCode);
            httpServletResponse.setContentType("application/json;charset=utf-8");
            httpServletResponse.setContentLength(errorBytes.length);
            httpServletResponse.getOutputStream().write(errorBytes);
        } else {
            httpServletResponse.sendError(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                    ex.getMessage());
        }
    }
}
