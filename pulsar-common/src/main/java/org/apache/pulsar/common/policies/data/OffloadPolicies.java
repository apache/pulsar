package org.apache.pulsar.common.policies.data;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Definition of the offload policies.
 */
public class OffloadPolicies {

    public final static int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024 * 1024;   // 64MB
    public final static int READ_BUFFER_SIZE_IN_BYTES = 1024 * 1024;      // 1MB

    private String region;
    private String bucket;
    private String endpoint;
    private int maxBlockSizeInBytes;
    private int readBufferSizeInBytes;

    public OffloadPolicies() {

    }

    public OffloadPolicies(String region, String bucket, String endpoint) {
        this(region, bucket, endpoint, MAX_BLOCK_SIZE_IN_BYTES, READ_BUFFER_SIZE_IN_BYTES);
    }

    public OffloadPolicies(String region, String bucket, String endpoint,
                           int maxBlockSizeInBytes, int readBufferSizeInBytes) {
        this.region = region;
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.maxBlockSizeInBytes = maxBlockSizeInBytes;
        this.readBufferSizeInBytes = readBufferSizeInBytes;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public int getMaxBlockSizeInBytes() {
        return maxBlockSizeInBytes;
    }

    public int getReadBufferSizeInBytes() {
        return readBufferSizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, bucket, endpoint, maxBlockSizeInBytes, readBufferSizeInBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OffloadPolicies other = (OffloadPolicies) obj;
        return Objects.equals(region, other.getRegion())
                && Objects.equals(bucket, other.getBucket())
                && Objects.equals(endpoint, other.getEndpoint())
                && Objects.equals(maxBlockSizeInBytes, other.getMaxBlockSizeInBytes())
                && Objects.equals(readBufferSizeInBytes, other.getReadBufferSizeInBytes());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("region", region)
                .add("bucket", bucket)
                .add("endpoint", endpoint)
                .add("maxBlockSizeInBytes", maxBlockSizeInBytes)
                .add("readBufferSizeInBytes", readBufferSizeInBytes)
                .toString();
    }

}
