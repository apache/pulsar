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
package org.apache.pulsar.client.admin.internal;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BaseResourceTest {
    @Test
    public void testGetReasonFromServer() throws JsonProcessingException {
        // when readEntity(ErrorData.class) works
        String reason = "reason in response";
        String errMsg = "error message";
        Response response = Mockito.mock(Response.class);
        WebApplicationException e = new WebApplicationException(errMsg, response);
        Mockito.when(response.readEntity(ErrorData.class)).thenReturn(new ErrorData(reason));
        assertEquals(BaseResource.getReasonFromServer(e), reason);

        // fallback to e.getResponse().readEntity(String.class)
        response = Mockito.mock(Response.class);
        Mockito.when(response.readEntity(ErrorData.class)).thenThrow(new RuntimeException());
        Mockito.when(response.readEntity(String.class))
                .thenReturn(ObjectMapperFactory.getThreadLocal().writeValueAsString(new ErrorData(reason)));
        assertEquals(BaseResource.getReasonFromServer(new WebApplicationException(errMsg, response)), reason);

        // fallback to e.getMessage()
        response = Mockito.mock(Response.class);
        Mockito.when(response.readEntity(ErrorData.class)).thenThrow(new RuntimeException());
        Mockito.when(response.readEntity(String.class)).thenThrow(new RuntimeException());
        assertEquals(BaseResource.getReasonFromServer(new WebApplicationException(ObjectMapperFactory.getThreadLocal()
                .writeValueAsString(new ErrorData(reason)), response)), reason);

        // fallback to original e.getMessage()
        response = Mockito.mock(Response.class);
        Mockito.when(response.readEntity(ErrorData.class)).thenThrow(new RuntimeException());
        Mockito.when(response.readEntity(String.class)).thenThrow(new RuntimeException());
        assertEquals(BaseResource.getReasonFromServer(new WebApplicationException(errMsg, response)), errMsg);
    }
}
