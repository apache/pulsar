package org.apache.pulsar.functions.runtime.worker;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.runtime.worker.Worker;
import org.apache.pulsar.functions.runtime.worker.WorkerConfig;
import org.apache.pulsar.functions.runtime.worker.rest.WorkerServer;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class WorkerTest {

    private static void runWorker(String workerId, int port) throws PulsarClientException, URISyntaxException, InterruptedException {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerPort(port);
        workerConfig.setZookeeperUri(new URI("http://127.0.0.1:2181"));
        workerConfig.setNumFunctionPackageReplicas(1);
        workerConfig.setFunctionMetadataTopic("persistent://sample/standalone/ns1/fmt");
        workerConfig.setPulsarBrokerRootUrl("pulsar://localhost:6650");
        workerConfig.setWorkerId(workerId);
        Worker worker = new Worker(workerConfig);
        worker.start();
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
