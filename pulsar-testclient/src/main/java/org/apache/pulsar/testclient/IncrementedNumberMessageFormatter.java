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
package org.apache.pulsar.testclient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * message formatter used to generate long number from 0.
 * For non-txn test, IncrementedNumberMessageFormatter support for resending messages
 * when send failed.
 * For txn tset, IncrementedNumberMessageFormatter support for taking snapshot of current
 * Long value when the perf process is killed and restoring Long value from snapshot when
 * the perf process is started; support for persisting messages in transaction to local files
 * in case of txn abortion or process shutdown.
 */
public class IncrementedNumberMessageFormatter implements IMessageFormatter{
    private static final Logger log = LoggerFactory.getLogger(IncrementedNumberMessageFormatter.class);

    private AtomicLong atomicLong = new AtomicLong();

    // tmp files corresponding to txn aborted.
    public ConcurrentMap<File, LinkedList<byte[]>> tmpFilesWithAbortedTxn = new ConcurrentHashMap();

    // blocking queue used for resending messages in non-txn produce.
    public BlockingQueue<byte[]> msgToResend = new LinkedBlockingQueue<>();

    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    private static final String snapshotFilePath = "snapshot.data";

    public static synchronized byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return Arrays.copyOf(buffer.array(), Long.BYTES);
    }

    public static synchronized long bytesToLong(byte[] bytes) {
        buffer.clear(); // need clear to put
        buffer.put(bytes, 0, bytes.length);
        buffer.flip(); //need flip to get
        return buffer.getLong();
    }

    @Override
    public byte[] formatMessage(String producerName, long msg, byte[] message) {
        return longToBytes(atomicLong.getAndAdd(1L));
    }

    public byte[] formatMessage() throws InterruptedException {
        if (msgToResend.isEmpty()) {
            return longToBytes(atomicLong.getAndAdd(1L));
        } else {
            byte[] arr = msgToResend.poll(2, TimeUnit.SECONDS);
            if (arr == null) {
                return longToBytes(atomicLong.getAndAdd(1L));
            } else {
                return arr;
            }
        }
    }

    public byte[] formatMessage(File file) {
        if (tmpFilesWithAbortedTxn.containsKey(file)) {
            byte[] arr = tmpFilesWithAbortedTxn.get(file).removeFirst();
            if (tmpFilesWithAbortedTxn.get(file).size() == 0) {
                tmpFilesWithAbortedTxn.remove(file);
            }
            return arr;
        }
        return formatMessage(null, 0L, null);
    }

    public int registerTmpFileWithAbortedTxn(File file) throws IOException {
        // read content persisted in tmp file.
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] arr = new byte[Long.BYTES];
        LinkedList<byte[]> tmpContent = new LinkedList<>();
        while (fileInputStream.read(arr) != -1) {
            tmpContent.addLast(arr);
            arr = new byte[Long.BYTES];
        }
        fileInputStream.close();
        if (tmpContent.size() != 0) {
            tmpFilesWithAbortedTxn.put(file, tmpContent);
        }
        return tmpContent.size();
    }

    public boolean takeSnapshot(){
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(snapshotFilePath);
            fileOutputStream.write(longToBytes(atomicLong.get()));
            fileOutputStream.close();
            log.info("Message Formatter take snapshot successfully. value:{}", atomicLong.get());
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean recoverFromSnapshot() {
        try {
            File file = new File(snapshotFilePath);
            if (file.exists()) {
                FileInputStream fileInputStream = new FileInputStream(file);
                byte[] arr = new byte[Long.BYTES];
                fileInputStream.read(arr);
                atomicLong.compareAndSet(0, bytesToLong(arr));
                log.info("Message Formatter recover from snapshot file. value:{}", atomicLong.get());
                fileInputStream.close();
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public long getMessageCount() {
        return atomicLong.get();
    }
}
