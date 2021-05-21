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
package org.apache.pulsar.io.core;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.Record;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Pulsar's Batch Push Source interface. Batch Push Sources have the same lifecycle
 * as the regular BatchSource, aka discover, prepare. The reason its called Push is
 * because BatchPushSource can emit a record using the consume method that they
 * invoke whenever they have data to be published to Pulsar.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class BatchPushSource<T> implements BatchSource<T> {

    private static class NullRecord implements Record {
        @Override
        public Object getValue() {
            return null;
        }
    }

    private static class ErrorNotifierRecord implements Record {
        private Exception e;
        public ErrorNotifierRecord(Exception e) {
            this.e = e;
        }
        @Override
        public Object getValue() {
            return null;
        }

        public Exception getException() {
            return e;
        }
    }

    private LinkedBlockingQueue<Record<T>> queue;
    private static final int DEFAULT_QUEUE_LENGTH = 1000;
    private final NullRecord nullRecord = new NullRecord();

    public BatchPushSource() {
        this.queue = new LinkedBlockingQueue<>(this.getQueueLength());
    }

    @Override
    public Record<T> readNext() throws Exception {
        Record<T> record = queue.take();
        if (record instanceof ErrorNotifierRecord) {
            throw ((ErrorNotifierRecord) record).getException();
        }
        if (record instanceof NullRecord) {
            return null;
        } else {
            return record;
        }
    }

    /**
     * Send this message to be written to Pulsar.
     * Pass null if you you are done with this task
     * @param record next message from source which should be sent to a Pulsar topic
     */
    public void consume(Record<T> record) {
        try {
            if (record != null) {
                queue.put(record);
            } else {
                queue.put(nullRecord);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get length of the queue that records are push onto
     * Users can override this method to customize the queue length
     * @return queue length
     */
    public int getQueueLength() {
        return DEFAULT_QUEUE_LENGTH;
    }

    /**
     * Allows the source to notify errors asynchronously
     * @param ex
     */
    public void notifyError(Exception ex) {
        consume(new ErrorNotifierRecord(ex));
    }
}