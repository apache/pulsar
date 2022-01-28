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
package org.apache.pulsar.websocket;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.eclipse.jetty.websocket.servlet.UpgradeHttpServletRequest;

/**
 * WebSocket HttpServletRequest wrapper.
 */
public class WebSocketHttpServletRequestWrapper extends HttpServletRequestWrapper {

    static final String HTTP_HEADER_NAME = "Authorization";
    static final String HTTP_HEADER_VALUE_PREFIX = "Bearer ";
    static final String TOKEN = "token";

    public WebSocketHttpServletRequestWrapper(HttpServletRequest request) {
        super(request);
    }

    @Override
    public String getHeader(String name) {
        // The browser javascript WebSocket client couldn't add the auth param to the request header, use the
        // query param `token` to transport the auth token for the browser javascript WebSocket client.
        if (name.equals(HTTP_HEADER_NAME)
                && !((UpgradeHttpServletRequest) this.getRequest()).getHeaders().containsKey(HTTP_HEADER_NAME)) {
            String token = getRequest().getParameter(TOKEN);
            if (token != null && !token.startsWith(HTTP_HEADER_VALUE_PREFIX)) {
                return HTTP_HEADER_VALUE_PREFIX + token;
            }
            return token;
        }
        return super.getHeader(name);
    }
}
