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

package org.apache.pulsar.client.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Transaction;
import org.apache.pulsar.common.util.FutureUtil;

public abstract class TransactionBase extends HandlerState implements Transaction {

    public TransactionBase(PulsarClientImpl client, String topic) {
        super(client, topic);
    }

    CompletableFuture<PulsarClientException> checkStateAsync() {
        switch (getState()) {
            case Ready:
            case Connecting:
                break; // OK
            case Closing:
            case Closed:
                return FutureUtil.failedFuture(new PulsarClientException.AlreadyClosedException(""));
            case Terminated:
                return FutureUtil
                           .failedFuture(new PulsarClientException.AlreadyClosedException("Topic was terminated"));
            case Failed:
            case Uninitialized:
                return FutureUtil.failedFuture(new PulsarClientException.NotConnectedException());
        }
        return CompletableFuture.completedFuture(null);
    }

}
