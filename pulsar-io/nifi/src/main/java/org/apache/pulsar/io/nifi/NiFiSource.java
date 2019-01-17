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
package org.apache.pulsar.io.nifi;

import lombok.extern.slf4j.Slf4j;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * A source that pulls data from Apache NiFi using the NiFi Site-to-Site client. This source
 * produces NiFiDataPackets which encapsulate the content and attributes of a NiFi FlowFile.
 */
@Connector(
    name = "nifi",
    type = IOType.SOURCE,
    help = "A simple connector to move messages from Apache NiFi using the NiFi Site-to-Site client to a Pulsar topic",
    configClass = NiFiConfig.class)
@Slf4j
public class NiFiSource extends PushSource<NiFiDataPacket> {

    private long waitTimeMs;
    private volatile boolean isRunning = true;
    private NiFiConfig niFiConfig;
    private SiteToSiteClientConfig clientConfig;

    Thread runnerThread;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        niFiConfig = NiFiConfig.load(config);
        if (niFiConfig.getUrl() == null
                || niFiConfig.getPortName() == null) {
            throw new IllegalArgumentException("Required property not set.");
        }

        waitTimeMs = niFiConfig.getWaitTimeMs();
        clientConfig = new SiteToSiteClient.Builder()
                .url(niFiConfig.getUrl())
                .portName(niFiConfig.getPortName())
                .requestBatchCount(niFiConfig.getRequestBatchCount())
                .buildConfig();

        this.start();
    }

    @Override
    public void close() throws Exception {
        this.isRunning = false;
        if (runnerThread != null) {
            runnerThread.interrupt();
            runnerThread.join();
            runnerThread = null;
        }
    }

    protected void start() throws Exception {
        runnerThread = new Thread(new ReceiveRunnable());
        runnerThread.setName("Nifi Source Thread");
        runnerThread.start();
    }


    class ReceiveRunnable implements Runnable {

        public ReceiveRunnable() {
        }

        @Override
        public void run() {
            try {
                final SiteToSiteClient client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
                try {
                    while (isRunning) {
                        final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                        DataPacket dataPacket = transaction.receive();
                        if (dataPacket == null) {
                            transaction.confirm();
                            transaction.complete();

                            // no data available. Wait a bit and try again
                            try {
                                Thread.sleep(waitTimeMs);
                            } catch (InterruptedException e) {
                            }
                            continue;
                        }

                        final List<NiFiDataPacket> dataPackets = new ArrayList<>();
                        do {
                            // Read the data into a byte array and wrap it along with the attributes
                            // into a NiFiDataPacket.
                            final InputStream inStream = dataPacket.getData();
                            final byte[] data = new byte[(int) dataPacket.getSize()];
                            StreamUtils.fillBuffer(inStream, data);

                            final Map<String, String> attributes = dataPacket.getAttributes();
                            final NiFiDataPacket NiFiDataPacket = new StandardNiFiDataPacket(data, attributes);
                            dataPackets.add(NiFiDataPacket);
                            dataPacket = transaction.receive();
                        } while (dataPacket != null);

                        // Confirm transaction to verify the data
                        transaction.confirm();

                        for (NiFiDataPacket dp : dataPackets) {
                            consume(new NiFiRecord(dp));
                        }

                        transaction.complete();
                    }
                } finally {
                    try {
                        client.close();
                    } catch (final IOException ioe) {
                        log.warn("Failed to close client", ioe);
                    }
                }
            } catch (final IOException ioe) {
                log.warn("Failed to receive data from NiFi", ioe);
            }
        }
    }

    static private class NiFiRecord implements Record<NiFiDataPacket> {
        private final NiFiDataPacket value;

        public NiFiRecord(NiFiDataPacket value) {
            this.value = value;
        }

        @Override
        public NiFiDataPacket getValue() {
            return value;
        }
    }

}
