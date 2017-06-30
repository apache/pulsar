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
package org.apache.pulsar.zookeeper;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

@Aspect
public class FinalRequestProcessorAspect {

    private static final Map<Integer, String> requestTypeMap = new HashMap<>();

    static {
        // Prefill the map
        requestTypeMap.put(OpCode.notification, "notification");
        requestTypeMap.put(OpCode.create, "create");
        requestTypeMap.put(OpCode.delete, "delete");
        requestTypeMap.put(OpCode.exists, "exists");
        requestTypeMap.put(OpCode.getData, "getData");
        requestTypeMap.put(OpCode.setData, "setData");
        requestTypeMap.put(OpCode.getACL, "getACL");
        requestTypeMap.put(OpCode.setACL, "setACL");
        requestTypeMap.put(OpCode.getChildren, "getChildren");
        requestTypeMap.put(OpCode.sync, "sync");
        requestTypeMap.put(OpCode.ping, "ping");
        requestTypeMap.put(OpCode.getChildren2, "getChildren2");
        requestTypeMap.put(OpCode.check, "check");
        requestTypeMap.put(OpCode.multi, "multi");
        requestTypeMap.put(OpCode.auth, "auth");
        requestTypeMap.put(OpCode.setWatches, "setWatches");
        requestTypeMap.put(OpCode.sasl, "sasl");
        requestTypeMap.put(OpCode.createSession, "createSession");
        requestTypeMap.put(OpCode.closeSession, "closeSession");
        requestTypeMap.put(OpCode.error, "error");
    }

    private static final Counter requests = Counter
            .build("zookeeper_server_requests", "Requests issued to a particular server").labelNames("type").create()
            .register();

    private static final Summary requestsLatency = Summary.build().name("zookeeper_server_requests_latency_ms")
            .help("Requests latency in millis") //
            .quantile(0.50, 0.01) //
            .quantile(0.75, 0.01) //
            .quantile(0.95, 0.01) //
            .quantile(0.99, 0.01) //
            .quantile(0.999, 0.01) //
            .quantile(0.9999, 0.01) //
            .quantile(1.0, 0.01) //
            .maxAgeSeconds(60) //
            .labelNames("type") //
            .create().register();

    @Pointcut("execution(void org.apache.zookeeper.server.FinalRequestProcessor.processRequest(..))")
    public void processRequest() {
    }

    @Around("processRequest()")
    public void timedProcessRequest(ProceedingJoinPoint joinPoint) throws Throwable {
        joinPoint.proceed();

        Request request = (Request) joinPoint.getArgs()[0];

        String type = requestTypeMap.getOrDefault(request.type, "unknown");
        requests.labels(type).inc();

        long latencyMs = System.currentTimeMillis() - request.createTime;
        String latencyLabel = isWriteRequest(request.type) ? "write" : "read";
        requestsLatency.labels(latencyLabel).observe(latencyMs);
    }

    private static boolean isWriteRequest(int opCode) {
        switch (opCode) {
        case OpCode.create:
        case OpCode.delete:
        case OpCode.setData:
        case OpCode.setACL:
        case OpCode.sync:
        case OpCode.createSession:
        case OpCode.closeSession:
            return true;

        case OpCode.notification:
        case OpCode.exists:
        case OpCode.getData:
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.ping:
        case OpCode.getChildren2:
        case OpCode.check:
        case OpCode.multi:
        case OpCode.auth:
        case OpCode.setWatches:
        case OpCode.sasl:
        case OpCode.error:
        default:
            return false;
        }
    }
}
