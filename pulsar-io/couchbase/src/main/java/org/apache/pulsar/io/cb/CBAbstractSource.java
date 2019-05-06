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
package org.apache.pulsar.io.cb;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.dcp.config.DcpControl.Names.ENABLE_NOOP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
public abstract class CBAbstractSource<T> extends PushSource<T> {
    protected Thread thread = null;
    protected volatile boolean running = false;

    private CBSourceConfig cbSourceConfig;

    private Client client;

    protected final Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread thread, Throwable ex) {
            log.error("{} has an error", thread.getName(), ex);
        }
    };

    /**
     * Open connector with configuration
     *
     * @param config        initialization config
     * @param sourceContext
     * @throws Exception IO type exceptions when opening a connector
     */
    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        log.info("opening connection to couchbase.");

        cbSourceConfig = CBSourceConfig.load(config);
//        cbSourceConfig.validate();

        DefaultConnectionNameGenerator.INSTANCE.name();
        client = Client.configure()
                .connectionNameGenerator(DefaultConnectionNameGenerator.forProduct(
                        "pulsar-couchbase-connector", "version1",
                        cbSourceConfig.getConnectionName()))
                .connectTimeout(cbSourceConfig.getConnectionTimeOut())
                .hostnames(cbSourceConfig.getHostnames())
                .bucket(cbSourceConfig.getBucket())
                .username(cbSourceConfig.getUsername())
                .password(cbSourceConfig.getPassword())
                .controlParam(ENABLE_NOOP, "true")
                .compression(getCompressedMode())
                .mitigateRollbacks(cbSourceConfig.getPersistencePollingInterval(), MILLISECONDS)
                .flowControl(cbSourceConfig.getFlowControlBufferSizeInBytes())
                .bufferAckWatermark(60)
                .sslEnabled(cbSourceConfig.getSslEnabled())
                .sslKeystoreFile(cbSourceConfig.getSslKeystoreFile())
                .sslKeystorePassword(cbSourceConfig.getSslKeystorePassword())
                .build();

        log.info("opened connection to couchbase successfully.");
    }

    protected void process() {
        while (running) {
            client.dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                    byte[] message = null;

                    if (DcpMutationMessage.is(event)) {
                        message = DcpMutationMessage.contentBytes(event);
                    }
                    else if (DcpDeletionMessage.is(event)) {
                        message = MessageUtil.getContentAsByteArray(event);
                    }

                    try {
                        CBRecord cbRecord = new CBRecord();
                        cbRecord.setRecord(extractValue(message));
                        consume(cbRecord);

                        event.release();
                    }
                    catch (Exception ex) {
                        log.error("process error!", ex);

                        event.retain();
                    }
                }
            });

            client.controlEventHandler(new ControlEventHandler() {
                @Override
                public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                    log.info("Control Event: {}", event.toString());

                    event.release();
                }
            });

            client.systemEventHandler(new SystemEventHandler() {
                @Override
                public void onEvent(CouchbaseEvent event) {
                    log.info("System Event: {}", event.type().toString());
                }
            });
        }
    }

    protected void start() {
        Objects.requireNonNull(client, "client is null");

        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

        thread.setName("CouchBase-IO-Connector");
        thread.setUncaughtExceptionHandler(handler);

        running = true;

        thread.start();
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        log.info("closing client connection to couchbase server.");

        if (client != null) {
            client.disconnect().await();
        }

        if (!running) {
            return;
        }

        running = false;

        if (thread != null) {
            thread.interrupt();
            thread.join();
        }

        log.info("closed client connection to couchbase server successfully.");
    }

    public abstract T extractValue(byte[] bytes);

    @Getter
    @Setter
    static private class CBRecord<T> implements Record<T> {
        private T record;
        private Long id;

        @Override
        public Optional<String> getKey() {
            return Optional.of(Long.toString(id));
        }

        @Override
        public T getValue() {
            return record;
        }
    }

    private CompressionMode getCompressedMode() {
        return CompressionMode.valueOf(cbSourceConfig.getCompressedMode());
    }
}
