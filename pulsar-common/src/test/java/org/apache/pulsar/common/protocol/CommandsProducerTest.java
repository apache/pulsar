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
package org.apache.pulsar.common.protocol;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.testng.annotations.Test;

public class CommandsProducerTest {

    @Test
    public void testNewAutoProduceBytesProducer() {
        ByteBuf frame = Commands.newProducer("persistent://public/default/topic", 1L, 2L, "producer", false,
                Map.of(), null, 0L, false, ProducerAccessMode.Shared, Optional.empty(), false, null, true);
        BaseCommand command = parseFrame(frame);

        assertEquals(command.getType(), BaseCommand.Type.PRODUCER);
        assertTrue(command.getProducer().isAutoProduceBytes());
    }

    @Test
    public void testConnectedAdvertisesAutoProduceBytesWithoutSchema() {
        BaseCommand command = parseFrame(Commands.newConnected(
                Commands.getCurrentProtocolVersion(), 1024, true, true, true));

        assertTrue(command.getConnected().getFeatureFlags().isSupportsAutoProduceBytesWithoutSchema());
    }

    @Test
    public void testConnectedCanDisableAutoProduceBytesWithoutSchema() {
        BaseCommand command = parseFrame(Commands.newConnected(
                Commands.getCurrentProtocolVersion(), 1024, true, true, true, false));

        assertFalse(command.getConnected().getFeatureFlags().isSupportsAutoProduceBytesWithoutSchema());
    }

    private static BaseCommand parseFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4);
            int commandSize = (int) frame.readUnsignedInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            command.materialize();
            return command;
        } finally {
            frame.release();
        }
    }
}
