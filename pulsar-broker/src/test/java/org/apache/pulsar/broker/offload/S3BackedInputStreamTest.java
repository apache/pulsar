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

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.offload.impl.S3BackedInputStreamImpl;

import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
class S3BackedInputStreamTest extends S3TestBase {
    class RandomInputStream extends InputStream {
        final Random r;
        int bytesRemaining;

        RandomInputStream(int seed, int bytesRemaining) {
            this.r = new Random(seed);
            this.bytesRemaining = bytesRemaining;
        }

        @Override
        public int read() {
            if (bytesRemaining-- > 0) {
                return r.nextInt() & 0xFF;
            } else {
                return -1;
            }
        }
    }

    private void assertStreamsMatch(InputStream a, InputStream b) throws Exception {
        int ret = 0;
        while (ret >= 0) {
            ret = a.read();
            Assert.assertEquals(ret, b.read());
        }
        Assert.assertEquals(-1, a.read());
        Assert.assertEquals(-1, b.read());
    }

    private void assertStreamsMatchByBytes(InputStream a, InputStream b) throws Exception {
        byte[] bytesA = new byte[100];
        byte[] bytesB = new byte[100];

        int retA = 0;
        while (retA >= 0) {
            retA = a.read(bytesA, 0, 100);
            int retB = b.read(bytesB, 0, 100);
            Assert.assertEquals(retA, retB);
            Assert.assertEquals(bytesA, bytesB);
        }
    }

    @Test
    public void testReadingFullObject() throws Exception {
        String objectKey = "foobar";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectSize);
        s3client.putObject(BUCKET, objectKey, toWrite, metadata);

        BackedInputStream toTest = new S3BackedInputStreamImpl(s3client, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        assertStreamsMatch(toTest, toCompare);
    }

    @Test
    public void testReadingFullObjectByBytes() throws Exception {
        String objectKey = "foobar";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectSize);
        s3client.putObject(BUCKET, objectKey, toWrite, metadata);

        BackedInputStream toTest = new S3BackedInputStreamImpl(s3client, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        assertStreamsMatchByBytes(toTest, toCompare);
    }

    @Test(expectedExceptions = IOException.class)
    public void testErrorOnS3Read() throws Exception {
        BackedInputStream toTest = new S3BackedInputStreamImpl(s3client, BUCKET, "doesn't exist",
                                                                 (key, md) -> {},
                                                                 1234, 1000);
        toTest.read();
    }


    @Test
    public void testSeek() throws Exception {
        String objectKey = "foobar";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        Map<Integer, InputStream> seeks = new HashMap<>();
        Random r = new Random(12345);
        for (int i = 0; i < 20; i++) {
            int seek = r.nextInt(objectSize+1);
            RandomInputStream stream = new RandomInputStream(0, objectSize);
            stream.skip(seek);
            seeks.put(seek, stream);
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectSize);
        s3client.putObject(BUCKET, objectKey, toWrite, metadata);

        BackedInputStream toTest = new S3BackedInputStreamImpl(s3client, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);
        for (Map.Entry<Integer, InputStream> e : seeks.entrySet()) {
            toTest.seek(e.getKey());
            assertStreamsMatch(toTest, e.getValue());
        }
    }

    @Test
    public void testSeekWithinCurrent() throws Exception {
        String objectKey = "foobar";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectSize);
        s3client.putObject(BUCKET, objectKey, toWrite, metadata);

        AmazonS3 spiedClient = spy(s3client);
        BackedInputStream toTest = new S3BackedInputStreamImpl(spiedClient, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);

        // seek forward
        RandomInputStream firstSeek = new RandomInputStream(0, objectSize);
        toTest.seek(100);
        firstSeek.skip(100);
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(firstSeek.read(), toTest.read());
        }

        // seek forward a bit more, but in same block
        RandomInputStream secondSeek = new RandomInputStream(0, objectSize);
        toTest.seek(600);
        secondSeek.skip(600);
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(secondSeek.read(), toTest.read());
        }

        // seek back
        RandomInputStream thirdSeek = new RandomInputStream(0, objectSize);
        toTest.seek(200);
        thirdSeek.skip(200);
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(thirdSeek.read(), toTest.read());
        }

        verify(spiedClient, times(1)).getObject(anyObject());
    }

    @Test
    public void testSeekForward() throws Exception {
        String objectKey = "foobar";
        int objectSize = 12345;
        RandomInputStream toWrite = new RandomInputStream(0, objectSize);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectSize);
        s3client.putObject(BUCKET, objectKey, toWrite, metadata);

        BackedInputStream toTest = new S3BackedInputStreamImpl(s3client, BUCKET, objectKey,
                                                                 (key, md) -> {},
                                                                 objectSize, 1000);

        // seek forward to middle
        long middle = objectSize/2;
        toTest.seekForward(middle);

        try {
            long before = middle - objectSize/4;
            toTest.seekForward(before);
            Assert.fail("Shound't be able to seek backwards");
        } catch (IOException ioe) {
            // correct
        }

        long after = middle + objectSize/4;
        RandomInputStream toCompare = new RandomInputStream(0, objectSize);
        toCompare.skip(after);

        toTest.seekForward(after);
        assertStreamsMatch(toTest, toCompare);
    }
}
