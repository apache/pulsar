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
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.io.IOException;
import java.util.Map;


/**
 * A sink that delivers data to Apache NiFi using the NiFi Site-to-Site client. The sink requires
 * a NiFiDataPacketBuilder which can create instances of NiFiDataPacket from the incoming data.
 */
@Connector(
    name = "nifi",
    type = IOType.SINK,
    help = "The NiFiSink is used for moving messages from Pulsar to Apache NiFi using the NiFi Site-to-Site client.",
    configClass = NiFiConfig.class
)
@Slf4j
public class NiFiSink implements Sink<NiFiDataPacket> {

    private NiFiConfig niFiConfig;
    private SiteToSiteClient client;
    private SiteToSiteClientConfig clientConfig;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        niFiConfig = NiFiConfig.load(config);
        if (niFiConfig.getUrl() == null
                || niFiConfig.getPortName() == null
                || niFiConfig.getRequestBatchCount() <=0) {
            throw new IllegalArgumentException("Required property not set.");
        }

        clientConfig = new SiteToSiteClient.Builder()
                .url(niFiConfig.getUrl())
                .portName(niFiConfig.getPortName())
                .requestBatchCount(niFiConfig.getRequestBatchCount())
                .buildConfig();
        client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
    }

    @Override
    public void write(Record<NiFiDataPacket> record) {
        Transaction transaction = null;
        try {
            transaction = client.createTransaction(TransferDirection.SEND);
        } catch (IOException e) {
            log.error("create Transaction exception ", e);
        }
        if (transaction == null) {
            throw new IllegalStateException("Unable to create a NiFi Transaction to send data");
        }
        NiFiDataPacket niFiDataPacket = record.getValue();

        if (log.isDebugEnabled()) {
            log.debug("Record sending to kafka, record={}.", record);
        }

        try {
            transaction.send(niFiDataPacket.getContent(), niFiDataPacket.getAttributes());
            record.ack();

            transaction.confirm();
            transaction.complete();
        } catch (IOException e) {
            log.error("Got exception ", e);
            record.fail();
        }

    }

    @Override
    public void close() throws Exception {
        if (null  != client) {
            client.close();
        }
    }
}
