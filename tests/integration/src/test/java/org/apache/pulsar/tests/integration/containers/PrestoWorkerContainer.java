package org.apache.pulsar.tests.integration.containers;

/**
 * A pulsar container that runs the presto worker
 */
public class PrestoWorkerContainer extends PulsarContainer<PrestoWorkerContainer> {

    public static final String NAME = "presto-worker";
    public static final int PRESTO_HTTP_PORT = 8081;

    public PrestoWorkerContainer(String clusterName, String hostname) {
        super(
                clusterName,
                hostname,
                hostname,
                "bin/run-presto-worker.sh",
                -1,
                PRESTO_HTTP_PORT,
                "/v1/node");
    }
}
