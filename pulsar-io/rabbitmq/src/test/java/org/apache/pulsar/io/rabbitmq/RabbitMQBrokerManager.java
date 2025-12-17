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

import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

public class RabbitMQBrokerManager {
    private RabbitMQContainer rabbitMQContainer;

    public void startBroker() throws Exception {
        rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.7.25-management-alpine"));
        rabbitMQContainer.withVhost("default");
        rabbitMQContainer.start();
    }

    public void stopBroker() {
        if (rabbitMQContainer != null) {
            rabbitMQContainer.stop();
            rabbitMQContainer = null;
        }
    }

    public int getPort() {
        return rabbitMQContainer.getAmqpPort();
    }

    public String getUser() {
        return rabbitMQContainer.getAdminUsername();
    }

    public String getPassword() {
        return rabbitMQContainer.getAdminPassword();
    }
}
