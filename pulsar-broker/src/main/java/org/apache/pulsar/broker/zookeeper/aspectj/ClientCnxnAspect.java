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
package org.apache.pulsar.broker.zookeeper.aspectj;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetMaxChildrenRequest;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetMaxChildrenRequest;
import org.apache.zookeeper.proto.SetSASLRequest;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Lists;

@Aspect
public class ClientCnxnAspect {

    public static enum EventType {
        write, read, other;
    }
    
    private static ExecutorService eventProcessExecutor;

    public static interface EventListner {
        public void recordLatency(EventType eventType, long latencyMiliSecond);
    }

    private static final List<EventListner> listeners = Lists.newArrayList();

    @Pointcut("execution(* org.apache.zookeeper.ClientCnxn.*.processEvent(..))")
    public void processEvent() {
    }

    @Around("processEvent()")
    public void timedProcessEvent(ProceedingJoinPoint joinPoint) throws Throwable {
        joinPoint.proceed();
        // zkResponse event shouldn't be blocked and it should be processed
        // async
        if (eventProcessExecutor != null && !eventProcessExecutor.isShutdown()) {
            eventProcessExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    processEvent(joinPoint);
                }
            });
        }
    }

    private void processEvent(ProceedingJoinPoint joinPoint) {
        long startTimeMs = getStartTime(joinPoint.getArgs()[0]);
        if (startTimeMs == -1) {
            // couldn't find start time
            return;
        }
        Record request = getEventType(joinPoint.getArgs()[0]);

        if (request != null) {
            long timeElapsed = (MathUtils.now() - startTimeMs);
            notifyListeners(checkType(request), timeElapsed);
        }
    }

    private void notifyListeners(EventType eventType, long timeElapsed) {
        listeners.forEach(listener -> {
            try {
                listener.recordLatency(eventType, timeElapsed);
            } catch (Exception e) {
                LOG.warn("Listener failed to record latency ", e);
            }
        });
    }

    private EventType checkType(Record response) {

        if (response == null) {
            return EventType.other;
        } else if (response instanceof ConnectRequest) {
            return EventType.write;
        } else if (response instanceof CreateRequest) {
            return EventType.write;
        } else if (response instanceof DeleteRequest) {
            return EventType.write;
        } else if (response instanceof SetDataRequest) {
            return EventType.write;
        } else if (response instanceof SetACLRequest) {
            return EventType.write;
        } else if (response instanceof SetMaxChildrenRequest) {
            return EventType.write;
        } else if (response instanceof SetSASLRequest) {
            return EventType.write;
        } else if (response instanceof SetWatches) {
            return EventType.write;
        } else if (response instanceof SyncRequest) {
            return EventType.write;
        } else if (response instanceof ExistsRequest) {
            return EventType.read;
        } else if (response instanceof GetDataRequest) {
            return EventType.read;
        } else if (response instanceof GetMaxChildrenRequest) {
            return EventType.read;
        } else if (response instanceof GetACLRequest) {
            return EventType.read;
        } else if (response instanceof GetChildrenRequest) {
            return EventType.read;
        } else if (response instanceof GetChildren2Request) {
            return EventType.read;
        } else if (response instanceof GetSASLRequest) {
            return EventType.read;
        } else {
            return EventType.other;
        }
    }

    private long getStartTime(Object packet) {
        try {
            if (packet.getClass().getName().equals("org.apache.zookeeper.ClientCnxn$Packet")) {
                Field ctxField = Class.forName("org.apache.zookeeper.ClientCnxn$Packet").getDeclaredField("ctx");
                ctxField.setAccessible(true);
                Object zooworker = ctxField.get(packet);
                if (zooworker != null
                        && zooworker.getClass().getName().equals("org.apache.bookkeeper.zookeeper.ZooWorker")) {
                    Field timeField = Class.forName("org.apache.bookkeeper.zookeeper.ZooWorker")
                            .getDeclaredField("startTimeMs");
                    timeField.setAccessible(true);
                    long startTime = (long) timeField.get(zooworker);
                    return startTime;
                }
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to get event-start-time from zk-response", e);
            }
        }
        return -1;
    }

    private Record getEventType(Object packet) {
        try {
            if (packet.getClass().getName().equals("org.apache.zookeeper.ClientCnxn$Packet")) {
                Field field = Class.forName("org.apache.zookeeper.ClientCnxn$Packet").getDeclaredField("request");
                field.setAccessible(true);
                Record response = (Record) field.get(packet);
                return response;
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Failed to get event-type from zk-response", e);
            }
        }

        return null;
    }

    public static void registerExecutor(ExecutorService executor) {
        eventProcessExecutor = executor;
    }
    
    public static void addListener(EventListner listener) {
        listeners.add(listener);
    }
    
    public static void removeListener(EventListner listener) {
        listeners.remove(listener);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxnAspect.class);

}