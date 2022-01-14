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
package org.apache.pulsar.io.canal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import java.util.List;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 * A Simple class for mysql binlog sync to pulsar.
 */
@Connector(
    name = "canal",
    type = IOType.SOURCE,
    help = "The CanalByteSource is used for syncing mysql binlog to Pulsar.",
    configClass = CanalSourceConfig.class)
public class CanalByteSource extends CanalAbstractSource<byte[]> {

    @Override
    public Long getMessageId(Message message) {
        return message.getId();
    }

    @Override
    public byte[] extractValue(List<FlatMessage> flatMessages) {
        String messages = JSON.toJSONString(flatMessages, SerializerFeature.WriteMapNullValue);
        return messages.getBytes();
    }

}
