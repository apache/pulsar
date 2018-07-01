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
package org.apache.pulsar.broker.offload;

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import com.google.common.collect.ComparisonChain;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * Minimal mock for amazon client.
 * If making any changes, validate they behave the same as S3 by running all S3 tests with -DtestRealAWS=true
 */
class S3Mock extends AbstractAmazonS3 {
    @Override
    public boolean doesBucketExistV2(String bucketName) {
        return buckets.containsKey(bucketName);
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectName) {
        return buckets.containsKey(bucketName) && getBucket(bucketName).hasObject(objectName);
    }

    @Override
    public Bucket createBucket(String bucketName) {
        return buckets.computeIfAbsent(bucketName, (k) -> new MockBucket(k));
    }

    private MockBucket getBucket(String bucketName) throws AmazonS3Exception {
        MockBucket bucket = buckets.get(bucketName);
        if (bucket != null) {
            return bucket;
        } else {
            throw new AmazonS3Exception("NoSuchBucket: Bucket doesn't exist");
        }
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws AmazonS3Exception {
        return getBucket(putObjectRequest.getBucketName()).putObject(putObjectRequest);
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest) {
        return getBucket(getObjectRequest.getBucketName()).getObject(getObjectRequest);
    }

    @Override
    public ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
            throws AmazonS3Exception {
        return getBucket(getObjectMetadataRequest.getBucketName()).getObjectMetadata(getObjectMetadataRequest);
    }

    @Override
    public void deleteObject(DeleteObjectRequest deleteObjectRequest)
            throws AmazonS3Exception {
        getBucket(deleteObjectRequest.getBucketName()).deleteObject(deleteObjectRequest.getKey());
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
            throws AmazonS3Exception {
        List<DeleteObjectsResult.DeletedObject> results = deleteObjectsRequest.getKeys().stream().map((k) -> {
                getBucket(deleteObjectsRequest.getBucketName()).deleteObject(k.getKey());
                DeleteObjectsResult.DeletedObject res = new DeleteObjectsResult.DeletedObject();
                res.setKey(k.getKey());
                return res;
            }).collect(Collectors.toList());
        return new DeleteObjectsResult(results);
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
            throws AmazonS3Exception {
        S3Object from = getObject(new GetObjectRequest(copyObjectRequest.getSourceBucketName(),
                                                       copyObjectRequest.getSourceKey()));
        ObjectMetadata newMetadata = copyObjectRequest.getNewObjectMetadata();
        if (newMetadata == null) {
            newMetadata = from.getObjectMetadata();
        }
        newMetadata.setContentLength(from.getObjectMetadata().getContentLength());
        putObject(new PutObjectRequest(copyObjectRequest.getDestinationBucketName(),
                                       copyObjectRequest.getDestinationKey(),
                                       from.getObjectContent(),
                                       newMetadata));
        return new CopyObjectResult();
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
            throws AmazonS3Exception {
        return getBucket(request.getBucketName()).initMultipart(request);
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request)
            throws AmazonS3Exception {
        return getBucket(request.getBucketName()).uploadPart(request);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
            throws AmazonS3Exception {
        return getBucket(request.getBucketName()).completeMultipart(request);
    }

    ConcurrentHashMap<String, MockBucket> buckets = new ConcurrentHashMap<>();

    static class MockBucket extends Bucket {
        ConcurrentHashMap<String, MockObject> objects = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, MockMultipart> inprogressMultipart = new ConcurrentHashMap<>();

        MockBucket(String name) {
            super(name);
        }

        boolean hasObject(String key) {
            return objects.containsKey(key);
        }

        PutObjectResult putObject(PutObjectRequest putObjectRequest) throws AmazonS3Exception {
            byte[] bytes = streamToBytes(putObjectRequest.getInputStream(),
                                         (int)putObjectRequest.getMetadata().getContentLength());
            objects.put(putObjectRequest.getKey(),
                        new MockObject(putObjectRequest.getMetadata(), bytes));
            return new PutObjectResult();
        }

        S3Object getObject(GetObjectRequest getObjectRequest) throws AmazonS3Exception {
            MockObject obj = objects.get(getObjectRequest.getKey());
            if (obj == null) {
                throw new AmazonS3Exception("Object doesn't exist");
            }

            S3Object s3obj = new S3Object();
            s3obj.setBucketName(getObjectRequest.getBucketName());
            s3obj.setKey(getObjectRequest.getKey());

            if (getObjectRequest.getRange() != null) {
                long[] range = getObjectRequest.getRange();
                int size = (int)(range[1] - range[0] + 1);
                ObjectMetadata metadata = obj.metadata.clone();
                metadata.setHeader("Content-Range",
                                   String.format("bytes %d-%d/%d",
                                                 range[0], range[1], size));
                s3obj.setObjectMetadata(metadata);
                s3obj.setObjectContent(new ByteArrayInputStream(obj.data, (int)range[0], size));
                return s3obj;
            } else {
                s3obj.setObjectMetadata(obj.metadata);
                s3obj.setObjectContent(new ByteArrayInputStream(obj.data));
                return s3obj;
            }
        }

        void deleteObject(String key) {
            objects.remove(key);
        }

        ObjectMetadata getObjectMetadata(GetObjectMetadataRequest getObjectMetadataRequest)
                throws AmazonS3Exception {
            MockObject obj = objects.get(getObjectMetadataRequest.getKey());
            if (obj == null) {
                throw new AmazonS3Exception("Object doesn't exist");
            }
            return obj.metadata;
        }

        InitiateMultipartUploadResult initMultipart(InitiateMultipartUploadRequest request)
                throws AmazonS3Exception {
            String uploadId = UUID.randomUUID().toString();
            inprogressMultipart.put(uploadId, new MockMultipart(request.getKey(),
                                                                request.getObjectMetadata()));
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setBucketName(request.getBucketName());
            result.setKey(request.getKey());
            result.setUploadId(uploadId);
            return result;
        }

        MockMultipart getMultipart(String uploadId, String key) throws AmazonS3Exception {
            MockMultipart multi = inprogressMultipart.get(uploadId);
            if (multi == null) {
                throw new AmazonS3Exception("No such upload " + uploadId);
            }
            if (!multi.key.equals(key)) {
                throw new AmazonS3Exception("Wrong key for upload " + uploadId
                                            + ", expected " + key
                                            + ", got " + multi.key);
            }
            return multi;
        }

        UploadPartResult uploadPart(UploadPartRequest request)
                throws AmazonS3Exception {
            MockMultipart multi = getMultipart(request.getUploadId(), request.getKey());
            byte[] bytes = streamToBytes(request.getInputStream(), (int)request.getPartSize());
            UploadPartResult result = new UploadPartResult();
            result.setPartNumber(request.getPartNumber());
            result.setETag(multi.addPart(request.getPartNumber(), bytes));
            return result;
        }

        CompleteMultipartUploadResult completeMultipart(CompleteMultipartUploadRequest request)
                throws AmazonS3Exception {
            MockMultipart multi = getMultipart(request.getUploadId(), request.getKey());
            inprogressMultipart.remove(request.getUploadId());
            objects.put(request.getKey(), multi.complete(request.getPartETags()));
            CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
            result.setBucketName(request.getBucketName());
            result.setKey(request.getKey());
            return result;
        }
    }

    private static byte[] streamToBytes(InputStream data, int length) throws AmazonS3Exception {
        byte[] bytes = new byte[length];
        try {
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte)data.read();
            }
        } catch (IOException ioe) {
            throw new AmazonS3Exception("Error loading data", ioe);
        }
        return bytes;
    }

    static class MockObject {
        final ObjectMetadata metadata;
        final byte[] data;
        final Map<Integer, long[]> partRanges;


        MockObject(ObjectMetadata metadata, byte[] data) {
            this(metadata, data, null);
        }

        MockObject(ObjectMetadata metadata, byte[] data, Map<Integer, long[]> partRanges) {
            this.metadata = metadata;
            this.data = data;
            this.partRanges = partRanges;
        }

    }

    static class MockMultipart {
        final String key;
        final ObjectMetadata metadata;
        final ConcurrentSkipListMap<PartETag, byte[]> parts = new ConcurrentSkipListMap<>(
                (etag1, etag2) -> ComparisonChain.start().compare(etag1.getPartNumber(),
                                                                  etag2.getPartNumber()).result());

        MockMultipart(String key, ObjectMetadata metadata) {
            this.key = key;
            this.metadata = metadata;
        }

        String addPart(int partNumber, byte[] bytes) {
            String etag = UUID.randomUUID().toString();
            parts.put(new PartETag(partNumber, etag), bytes);
            return etag;
        }

        MockObject complete(List<PartETag> tags) throws AmazonS3Exception {
            if (parts.size() != tags.size()
                || !parts.keySet().containsAll(tags)) {
                throw new AmazonS3Exception("Tags don't match uploaded parts");
            }

            int totalSize = parts.values().stream().map(v -> v.length).reduce(0, (acc, v) -> acc + v);
            byte[] full = new byte[totalSize];

            Map<Integer, long[]> partRanges = new HashMap<>();
            int start = 0;
            for (Map.Entry<PartETag, byte[]> e : parts.entrySet()) {
                int partLength = e.getValue().length;
                System.arraycopy(e.getValue(), 0, full, start, partLength);
                partRanges.put(e.getKey().getPartNumber(),
                               new long[] { start, start + partLength - 1 });
                start += partLength;
            }
            metadata.setContentLength(totalSize);
            return new MockObject(metadata, full, partRanges);
        }
    }
}
