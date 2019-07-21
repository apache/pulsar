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
package org.apache.pulsar.io.hdfs2.sink;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Syncable;
import org.apache.pulsar.functions.api.Record;

/**
 * A thread that runs in the background and acknowledges Records
 * after they have been written to disk.
 *
 * @param <V>
 */
public class HdfsSyncThread<V> extends Thread {

    private final Syncable stream;
    private final BlockingQueue<Record<V>> unackedRecords;
    private final long syncInterval;
    private boolean keepRunning = true;

    public HdfsSyncThread(Syncable stream, BlockingQueue<Record<V>> unackedRecords, long syncInterval) {
      this.stream = stream;
      this.unackedRecords = unackedRecords;
      this.syncInterval = syncInterval;
    }

    @Override
    public void run() {
       while (keepRunning) {
         try {
            Thread.sleep(syncInterval);
            ackRecords();
         } catch (InterruptedException e) {
            return;
         } catch (IOException e) {
            e.printStackTrace();
         }
       }
    }

    public final void halt() throws IOException, InterruptedException {
       keepRunning = false;
       ackRecords();
    }

    private void ackRecords() throws IOException, InterruptedException {

        if (CollectionUtils.isEmpty(unackedRecords)) {
           return;
        }

        synchronized (stream) {
          stream.hsync();
        }

        while (!unackedRecords.isEmpty()) {
          unackedRecords.take().ack();
        }
    }
}
