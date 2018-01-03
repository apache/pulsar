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
package org.apache.pulsar.functions.runtime.worker;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.junit.Test;

import java.net.URISyntaxException;

public class WorkerTest {

    private static void runWorker(String workerId, int port) throws PulsarClientException, URISyntaxException, InterruptedException {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerPort(port);
        workerConfig.setZookeeperServers("127.0.0.1:2181");
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setFunctionMetadataTopic("persistent://sample/standalone/ns1/fmt");
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setWorkerId(workerId);
        workerConfig.setDownloadDirectory("/tmp");
        Worker worker = new Worker(workerConfig, new LimitsConfig(-1, -1, -1, 1024));
        worker.startAsync();
        worker.awaitRunning();
    }

    @Test
    public void testWorkerServer() throws URISyntaxException, InterruptedException, PulsarClientException {

//        Thread worker1 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    runWorker("worker-1", 8001);
//                } catch (PulsarClientException | URISyntaxException | InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        Thread worker2 = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    runWorker("worker-2", 8002);
//                } catch (PulsarClientException | URISyntaxException | InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        worker1.setName("worker-1");
//        worker2.setName("worker-2");
//
//        worker1.start();
//        worker2.start();
//
//        worker1.join();
//        worker2.join();

    }
}
