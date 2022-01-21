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

package org.apache.pulsar.io.elasticsearch.testcontainers;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
// Toxiproxy container, which will be used as a TCP proxy
public class ElasticToxiproxiContainer extends ToxiproxyContainer {

    public static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.4");

    final ElasticsearchContainer container;
    ToxiproxyContainer.ContainerProxy proxy;

    public ElasticToxiproxiContainer(ElasticsearchContainer container, Network network) {
        super(TOXIPROXY_IMAGE);
        this.withNetwork(network);
        this.container = container;
    }

    @Override
    public void start() {
        log.info("Starting toxiproxy container");
        super.start();
        proxy = this.getProxy(container, 9200);
    }

    public String getHttpHostAddress() {
        Objects.nonNull(proxy);
        return proxy.getContainerIpAddress() + ":" + proxy.getProxyPort();
    }

    public ToxiproxyContainer.ContainerProxy getProxy() {
        Objects.nonNull(proxy);
        return proxy;
    }

    public void removeToxicAfterDelay(String toxicName, long delayMs) {
        Objects.nonNull(proxy);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    log.info("removing the toxic {}", toxicName);
                    proxy.toxics().get(toxicName).remove();
                } catch (IOException e) {
                    log.error("failed to remove toxic " + toxicName, e);
                }
            }
        }, delayMs);
    }

}
