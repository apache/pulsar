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

package org.apache.pulsar.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Simple abstract class for Aerospike sink.
 * Users need to implement extractKeyValue function to use this sink
 */
public abstract class AerospikeAbstractSink<K, V> implements Sink<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeAbstractSink.class);

    // ----- Runtime fields
    private AerospikeSinkConfig aerospikeSinkConfig;
    private AerospikeClient client;
    private WritePolicy writePolicy;
    private BlockingQueue<AWriteListener> queue;
    private NioEventLoops eventLoops;
    private EventLoop eventLoop;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        aerospikeSinkConfig = AerospikeSinkConfig.load(config);
        if (aerospikeSinkConfig.getSeedHosts() == null
                || aerospikeSinkConfig.getKeyspace() == null
                || aerospikeSinkConfig.getColumnName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        writePolicy = new WritePolicy();
        writePolicy.maxRetries = aerospikeSinkConfig.getRetries();
        writePolicy.setTimeout(aerospikeSinkConfig.getTimeoutMs());
        eventLoops = new NioEventLoops(new EventPolicy(), 1);
        eventLoop = eventLoops.next();
        createClient(eventLoops);
        queue = new LinkedBlockingDeque<>(aerospikeSinkConfig.getMaxConcurrentRequests());
        for (int i = 0; i < aerospikeSinkConfig.getMaxConcurrentRequests(); ++i) {
            queue.put(new AWriteListener(queue));
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }

        if (eventLoops != null) {
            eventLoops.close();
        }
        LOG.info("Connection Closed");
    }

    @Override
    public void write(Record<byte[]> record) {
        KeyValue<K, V> keyValue = extractKeyValue(record);
        Key key = new Key(aerospikeSinkConfig.getKeyspace(), aerospikeSinkConfig.getKeySet(),
                keyValue.getKey().toString());
        Bin bin = new Bin(aerospikeSinkConfig.getColumnName(), Value.getAsBlob(keyValue.getValue()));
        AWriteListener listener = null;
        try {
            listener = queue.take();
        } catch (InterruptedException ex) {
            record.fail();
            return;
        }
        listener.setContext(record);
        client.put(eventLoop, listener, writePolicy, key, bin);
    }

    private void createClient(NioEventLoops eventLoops) {
        String[] hosts = aerospikeSinkConfig.getSeedHosts().split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid Seed Hosts");
        }
        Host[] aeroSpikeHosts = new Host[hosts.length];
        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            aeroSpikeHosts[i] = new Host(hostPort[0], Integer.parseInt(hostPort[1]));
        }
        ClientPolicy policy = new ClientPolicy();
        if (aerospikeSinkConfig.getUserName() != null && !aerospikeSinkConfig.getUserName().isEmpty()
            && aerospikeSinkConfig.getPassword() != null && !aerospikeSinkConfig.getPassword().isEmpty()) {
            policy.user = aerospikeSinkConfig.getUserName();
            policy.password = aerospikeSinkConfig.getPassword();
        }
        policy.eventLoops = eventLoops;
        client = new AerospikeClient(policy, aeroSpikeHosts);
    }

    private class AWriteListener implements WriteListener {
        private Record<byte[]> context;
        private BlockingQueue<AWriteListener> queue;

        public AWriteListener(BlockingQueue<AWriteListener> queue) {
            this.queue = queue;
        }

        public void setContext(Record<byte[]> record) {
            this.context = record;
        }

        @Override
        public void onSuccess(Key key) {
            if (context != null) {
                context.ack();
            }
            try {
                queue.put(this);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while being added to the queue", ex);
            }
        }

        @Override
        public void onFailure(AerospikeException e) {
            if (context != null) {
                context.fail();
            }
            try {
                queue.put(this);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while being added to the queue", ex);
            }
        }
    }

    public abstract KeyValue<K, V> extractKeyValue(Record<byte[]> message);
}