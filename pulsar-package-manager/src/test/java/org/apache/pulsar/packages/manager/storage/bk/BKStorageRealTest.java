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
package org.apache.pulsar.packages.manager.storage.bk;

import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BKStorageRealTest {
    public static void main(String[] args) throws Exception {
//        ServiceConfiguration configuration = new ServiceConfiguration();
//        configuration.setZookeeperServers("localhost:2181");
//        BKPackageStorage storage = new BKPackageStorage(configuration);
//        System.out.println("Initialize done");

//        String logname = "test-3";
//        if (ns.logExists(logname)) {
//            System.out.println("delete log " + logname);
//            ns.deleteLog(logname);
//        }
//
//        ns.createLog(logname);
//
//        if (ns.logExists(logname)) {
//            System.out.println("log exist");
//        }
//
//
//        testWriteSync(ns, logname);
//
//        testReadSync(ns, logname);
//
//        try {
//            DistributedLogManager logManager = ns.openLog("xx2");
//            testWriteSync(ns, "xx2");
//            testReadSync(ns, "xx2");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        storage.closeAsync().get();

//        Thread.sleep(1000);
//        String s = "hello test";
//        storage.writeAsync("/test/1111111", new ByteArrayInputStream(s.getBytes())).get();
//        storage.closeAsync().get();
//        System.out.println("write done");
    }

    public static void testWriteSync(Namespace namespace, String name) throws IOException, ExecutionException, InterruptedException {
        System.out.println("begin write to the namespace " + name);
        DistributedLogManager distributedLogManager = namespace.openLog(name);
        AsyncLogWriter writer = distributedLogManager.openAsyncLogWriter().get();

        List<LogRecord> logRecords = new ArrayList<>();

        LogRecord logRecord = new LogRecord(1, "hello-message-1".getBytes());
//        writer.write(logRecord).get();



        LogRecord logRecord1 = new LogRecord(2, "hello-message-2".getBytes());
//        writer.write(logRecord1).get();
        LogRecord logRecord2 = new LogRecord(3, "hello-message-3".getBytes());
////        writer.write(logRecord2).get();
        LogRecord logRecord3 = new LogRecord(4, "hello-message-4".getBytes());
////        writer.write(logRecord3).get();
//
        logRecords.add(logRecord);
        logRecords.add(logRecord1);
        logRecords.add(logRecord2);
        logRecords.add(logRecord3);
        writer.writeBulk(logRecords).get().forEach(dlsnCompletableFuture -> dlsnCompletableFuture.join());

        writer.markEndOfStream().get();
        writer.asyncClose().get();
        distributedLogManager.close();
        System.out.println("write over");

    }

    public static void testReadSync(Namespace namespace, String name) throws Exception {
        DistributedLogManager distributedLogManager = namespace.openLog(name);
        AsyncLogReader reader = distributedLogManager.getAsyncLogReader(DLSN.InitialDLSN);

        System.out.println(distributedLogManager.getLastDLSN());
        System.out.println("-2-2-2-2-2-2-2-2");

//        AsyncLogReader reader1 = distributedLogManager.getAsyncLogReader(new DLSN(1, -1, 0));
//        LogRecordWithDLSN logRecordWithDLSN = reader1.readNext().get();
//        System.out.println(new String(logRecordWithDLSN.getPayload()));
        LogRecordWithDLSN log;
        CompletableFuture<Void> future = new CompletableFuture<>();

        testReader(future, 10, reader);

        future.get();
        reader.asyncClose().get();
        distributedLogManager.close();
    }

    public static void testReader(CompletableFuture<Void> future, int num, AsyncLogReader reader) {
        reader.readBulk(num)
            .whenComplete((logRecordWithDLSNS, throwable) -> {
                if (throwable != null) {
                    System.out.println(throwable.getMessage());
//                    System.out.println(throwable.getCause().getMessage());
                    System.out.println("xxxxx-------xxxxx-------xxxxxx");
                    future.complete(null);
                    return;
                }
                logRecordWithDLSNS.forEach(log -> {
                    System.out.println(log.getTransactionId());
                    System.out.println(log.getDlsn());
                    System.out.println(new String(log.getPayload()));
                    System.out.println("-----next------");
                });
                testReader(future, num, reader);
            });
    }
}
