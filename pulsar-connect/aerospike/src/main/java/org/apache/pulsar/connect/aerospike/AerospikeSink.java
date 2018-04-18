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

package org.apache.pulsar.connect.aerospike;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import com.aerospike.client.*;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.apache.pulsar.common.util.KeyValue;
import org.apache.pulsar.connect.core.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple AeroSpike sink
 */
public class AerospikeSink<K, V> implements Sink<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeSink.class);

    // ----- Runtime fields
    private AerospikeSinkConfig aerospikeSinkConfig;
    private AerospikeClient client;
    private WritePolicy writePolicy;
    private BlockingQueue<AWriteListener> queue;
    private EventLoop eventLoop;

    @Override
    public void open(Map<String, String> config) throws Exception {
        aerospikeSinkConfig = AerospikeSinkConfig.load(config);
        if (aerospikeSinkConfig.getSeedHosts() == null
                || aerospikeSinkConfig.getKeyspace() == null
                || aerospikeSinkConfig.getColumnName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        writePolicy = new WritePolicy();
        writePolicy.maxRetries = aerospikeSinkConfig.getRetries();
        writePolicy.setTimeout(aerospikeSinkConfig.getTimeoutMs());
        createClient();
        queue = new LinkedBlockingDeque<>(aerospikeSinkConfig.getMaxConcurrentRequests());
        for (int i = 0; i < aerospikeSinkConfig.getMaxConcurrentRequests(); ++i) {
            queue.put(new AWriteListener(queue));
        }
        eventLoop = new NioEventLoops(new EventPolicy(), 1).next();
    }

    @Override
    public void close() throws Exception {
        client.close();
        LOG.info("Connection Closed");
    }

    @Override
    public CompletableFuture<Void> write(KeyValue<K, V> tuple) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Key key = new Key(aerospikeSinkConfig.getKeyspace(), aerospikeSinkConfig.getKeySet(), tuple.getKey().toString());
        Bin bin = new Bin(aerospikeSinkConfig.getColumnName(), Value.getAsBlob(tuple.getValue()));
        AWriteListener listener = null;
        try {
            listener = queue.take();
        } catch (InterruptedException ex) {
            future.completeExceptionally(ex);
            return future;
        }
        listener.setFuture(future);
        client.put(eventLoop, listener, writePolicy, key, bin);
        return future;
    }

    private void createClient() {
        String[] hosts = aerospikeSinkConfig.getSeedHosts().split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid Seed Hosts");
        }
        Host[] aeroSpikeHosts = new Host[hosts.length];
        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            aeroSpikeHosts[i] = new Host(hostPort[0], Integer.valueOf(hostPort[1]));
        }
        ClientPolicy policy = new ClientPolicy();
        if (aerospikeSinkConfig.getUserName() != null && !aerospikeSinkConfig.getUserName().isEmpty()
            && aerospikeSinkConfig.getPassword() != null && !aerospikeSinkConfig.getPassword().isEmpty()) {
            policy.user = aerospikeSinkConfig.getUserName();
            policy.password = aerospikeSinkConfig.getPassword();
        }
        client = new AerospikeClient(policy, aeroSpikeHosts);
    }

    private class AWriteListener implements WriteListener {
        private CompletableFuture<Void> future;
        private BlockingQueue<AWriteListener> queue;

        public AWriteListener(BlockingQueue<AWriteListener> queue) {
            this.queue = queue;
        }

        public void setFuture(CompletableFuture<Void> future) {
            this.future = future;
        }

        @Override
        public void onSuccess(Key key) {
            if (future != null) {
                future.complete(null);
            }
            try {
                queue.put(this);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while being added to the queue" ,ex);
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            if (future != null) {
                future.completeExceptionally(e);
            }
            try {
                queue.put(this);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while being added to the queue", ex);
            }
        }
    }
}