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
package org.apache.pulsar.functions.worker.rest.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.functions.worker.FunctionsStatsGenerator;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.functions.worker.rest.FunctionApiResource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/")
public class FunctionsMetricsResource extends FunctionApiResource {
    @Path("metrics")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getMetrics() throws JsonProcessingException {

        WorkerService workerService = get();

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);
            FunctionsStatsGenerator.generate(workerService,"default", stream);
            byte[] payload = buf.array();
            int arrayOffset = buf.arrayOffset();
            int readableBytes = buf.readableBytes();
            StreamingOutput streamOut = out -> {
                out.write(payload, arrayOffset, readableBytes);
                out.flush();
            };
            return Response
                .ok(streamOut)
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
        } finally {
            buf.release();
        }
    }
}
