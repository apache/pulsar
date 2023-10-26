package org.apache.pulsar.io.azuredataexplorer;

public class ADXSinkUtils {

    static final String INGEST_PREFIX = "ingest-";
    static final String PROTOCOL_SUFFIX = "://";

    static String getIngestionEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl;
        } else {
            return clusterUrl.replaceFirst(PROTOCOL_SUFFIX, PROTOCOL_SUFFIX + INGEST_PREFIX);
        }
    }

    static String getQueryEndpoint(String clusterUrl) {
        if (clusterUrl.contains(INGEST_PREFIX)) {
            return clusterUrl.replaceFirst(INGEST_PREFIX, "");
        } else {
            return clusterUrl;
        }
    }
}
