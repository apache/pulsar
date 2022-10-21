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

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;

public class HttpSecurityProcessor {
    public static void disableHttpDebugMethod(ServletContextHandler ctxHandler, String path) {
        Constraint c = new Constraint();
        c.setAuthenticate(true);
        ConstraintMapping traceMapping = new ConstraintMapping();
        traceMapping.setConstraint(c);
        traceMapping.setMethod("TRACE");
        traceMapping.setPathSpec(path);

        ConstraintMapping trackMapping = new ConstraintMapping();
        trackMapping.setConstraint(c);
        trackMapping.setMethod("TRACK");
        trackMapping.setPathSpec(path);
        ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
        securityHandler.setConstraintMappings(new ConstraintMapping[]{traceMapping, trackMapping});
        ctxHandler.setSecurityHandler(securityHandler);
    }
}
