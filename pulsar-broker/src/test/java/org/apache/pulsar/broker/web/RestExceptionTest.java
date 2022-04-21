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

import static org.testng.Assert.assertEquals;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.testng.annotations.Test;

/**
 * Unit test for pulsar functions.
 */
@Test(groups = "broker")
public class RestExceptionTest {

    @Test
    public void testRestException() {
        RestException re = new RestException(Status.TEMPORARY_REDIRECT, "test rest exception");
        RestException testException = new RestException(re);

        assertEquals(Status.TEMPORARY_REDIRECT.getStatusCode(), testException.getResponse().getStatus());
        assertEquals(re.getResponse().getEntity(), testException.getResponse().getEntity());
    }

    @Test
    public void testWebApplicationException() {
        WebApplicationException wae = new WebApplicationException("test web application exception", Status.TEMPORARY_REDIRECT);
        RestException testException = new RestException(wae);

        assertEquals(Status.TEMPORARY_REDIRECT.getStatusCode(), testException.getResponse().getStatus());
        assertEquals(wae.getResponse().getEntity(), testException.getResponse().getEntity());
    }

    @Test
    public void testOtherException() {
        Exception otherException = new Exception("test other exception");
        RestException testException = new RestException(otherException);

        assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), testException.getResponse().getStatus());
        ErrorData errorData = (ErrorData)testException.getResponse().getEntity();
        assertEquals(RestException.getExceptionData(otherException), errorData.reason);
    }

}
