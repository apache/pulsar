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
package org.apache.pulsar.client.api;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;

public class ReaderConfiguration implements Serializable {

    private int receiverQueueSize = 1000;

    private ReaderListener readerListener;

    private String readerName = null;

    /**
     * @return the configured {@link ReaderListener} for the reader
     */
    public ReaderListener getReaderListener() {
        return this.readerListener;
    }

    /**
     * Sets a {@link ReaderListener} for the reader
     * <p>
     * When a {@link ReaderListener} is set, application will receive messages through it. Calls to
     * {@link Reader#readNext()} will not be allowed.
     *
     * @param readerListener
     *            the listener object
     */
    public ReaderConfiguration setReaderListener(ReaderListener readerListener) {
        checkNotNull(readerListener);
        this.readerListener = readerListener;
        return this;
    }

    /**
     * @return the configure receiver queue size value
     */
    public int getReceiverQueueSize() {
        return this.receiverQueueSize;
    }

    /**
     * Sets the size of the consumer receive queue.
     * <p>
     * The consumer receive queue controls how many messages can be accumulated by the {@link Consumer} before the
     * application calls {@link Consumer#receive()}. Using a higher value could potentially increase the consumer
     * throughput at the expense of bigger memory utilization.
     * </p>
     * Default value is {@code 1000} messages and should be good for most use cases.
     *
     * @param receiverQueueSize
     *            the new receiver queue size value
     */
    public ReaderConfiguration setReceiverQueueSize(int receiverQueueSize) {
        checkArgument(receiverQueueSize >= 0, "Receiver queue size cannot be negative");
        this.receiverQueueSize = receiverQueueSize;
        return this;
    }

    /**
     * @return the consumer name
     */
    public String getReaderName() {
        return readerName;
    }

    /**
     * Set the consumer name.
     *
     * @param readerName
     */
    public ReaderConfiguration setReaderName(String readerName) {
        checkArgument(readerName != null && !readerName.equals(""));
        this.readerName = readerName;
        return this;
    }

    private static final long serialVersionUID = 1L;
}
