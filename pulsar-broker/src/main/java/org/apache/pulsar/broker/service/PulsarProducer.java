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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

import java.util.Map;

/**
 * Represents a currently connected producer
 */
public class PulsarProducer extends Producer {

    private final PulsarServerCnx cnx;

    public PulsarProducer(Topic topic, PulsarServerCnx cnx, long producerId, String producerName, String appId,
            boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
            boolean userProvidedProducerName) {
        super(topic, cnx, producerId, producerName, appId, isEncrypted, metadata, schemaVersion, epoch, userProvidedProducerName);
        this.cnx = cnx;
    }

    @Override
    protected void sendError(long producerId, long sequenceId, PulsarApi.ServerError serverError, String message) {
        cnx.ctx().writeAndFlush(Commands.newSendError(producerId, sequenceId, serverError, message));
    }

    @Override
    protected void execute(Runnable runnable) {
        cnx.ctx().channel().eventLoop().execute(runnable);
    }

    @Override
    protected void sendReceipt(long sequenceId, long highestSequenceId, long ledgerId, long entryId) {
        cnx.ctx().writeAndFlush(
                Commands.newSendReceipt(producerId, sequenceId, highestSequenceId, ledgerId, entryId),
                cnx.ctx().voidPromise());
    }
}
