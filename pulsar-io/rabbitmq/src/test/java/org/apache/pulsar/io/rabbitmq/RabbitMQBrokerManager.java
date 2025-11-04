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
package org.apache.pulsar.io.rabbitmq;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.qpid.server.SystemLauncher;
import org.apache.qpid.server.model.SystemConfig;

public class RabbitMQBrokerManager {

    private final SystemLauncher systemLauncher = new SystemLauncher();

    public void startBroker(String port) throws Exception {
        Map<String, Object> brokerOptions = getBrokerOptions(port);
        systemLauncher.startup(brokerOptions);
    }

    public void stopBroker() {
        systemLauncher.shutdown();
    }

    Map<String, Object> getBrokerOptions(String port) throws Exception {
        Path tmpFolder = Files.createTempDirectory("qpidWork");
        Map<String, Object> config = new HashMap<>();
        config.put("qpid.work_dir", tmpFolder.toAbsolutePath().toString());
        config.put("qpid.amqp_port", port);

        Map<String, Object> context = new HashMap<>();
        context.put(SystemConfig.INITIAL_CONFIGURATION_LOCATION, "classpath:qpid.json");
        context.put(SystemConfig.TYPE, "Memory");
        context.put(SystemConfig.CONTEXT, config);
        return context;
    }
}
