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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.logging.log4j.core.util.datetime.FixedDateFormat;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 * A Simple class for mysql binlog sync to pulsar.
 */
@Connector(
        name = "canal",
        type = IOType.SOURCE,
        help = "The CanalStringSource is used for syncing mysql binlog to Pulsar, easy to use presto sql search.",
        configClass = CanalSourceConfig.class)
public class CanalStringSource extends CanalAbstractSource<CanalMessage> {

    private Long messageId;

    @Override
    public Long getMessageId(Message message) {
        this.messageId = message.getId();
        return this.messageId;
    }

    @Override
    public CanalMessage extractValue(List<FlatMessage> flatMessages) {
        String messages = JSON.toJSONString(flatMessages, SerializerFeature.WriteMapNullValue);
        CanalMessage canalMessage = new CanalMessage();
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat(
            FixedDateFormat.FixedFormat.ISO8601_OFFSET_DATE_TIME_HHMM.getPattern());
        canalMessage.setTimestamp(dateFormat.format(date));
        canalMessage.setId(this.messageId);
        canalMessage.setMessage(messages);
        return canalMessage;
    }

}


@Data
@AllArgsConstructor
@NoArgsConstructor
class CanalMessage {
    private Long id;
    private String message;
    private String timestamp;
}
