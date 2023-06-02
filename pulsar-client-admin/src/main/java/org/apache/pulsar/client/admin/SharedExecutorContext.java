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
package org.apache.pulsar.client.admin;

import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Builder;
import lombok.Data;

/**
 * SharedExecutorContext if PulsarAdmin is created on the broker side,
 * this is used for passing shared executors for AsyncHttpConnector.
 */
@Builder
@Data
public class SharedExecutorContext {
    private EventLoopGroup eventLoopGroup;
    private Timer nettyTimer;
    private ScheduledExecutorService delayer;
}
