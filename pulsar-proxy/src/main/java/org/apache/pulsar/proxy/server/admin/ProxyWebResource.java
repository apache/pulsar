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
package org.apache.pulsar.proxy.server.admin;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.apache.pulsar.proxy.server.ProxyService;

public class ProxyWebResource {

    public static final String ATTRIBUTE_PROXY_SERVICE_NAME = "proxyService";
    public static final String ADMIN_PATH = "/admin";
    
    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    private ProxyService proxyService;
    
    
    protected ProxyService service() {
        if (proxyService == null) {
            proxyService = (ProxyService) servletContext.getAttribute(ATTRIBUTE_PROXY_SERVICE_NAME);
        }
        return proxyService;
    }

}
