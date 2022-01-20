package org.apache.pulsar.client.api;

public interface Progress {

    int compare(Progress progress);

    byte[] serialize();

    Progress deserialize(byte[] progressBuf);

}
