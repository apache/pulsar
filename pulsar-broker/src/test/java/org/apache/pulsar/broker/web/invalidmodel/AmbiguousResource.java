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
package org.apache.pulsar.broker.web.invalidmodel;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

/**
 * A resource with two methods for the same requests, which fails Jersey's validation of the resource model. It's alone
 * in its package, which WebServiceResourceModelTest scans like the broker scans the packages of its admin resources.
 */
@Path("/ambiguous")
public class AmbiguousResource {

    @GET
    public String get() {
        return "get";
    }

    @GET
    public String getAgain() {
        return "getAgain";
    }
}
