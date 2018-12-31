package org.apache.pulsar.io.canal;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.SimpleDateFormat;
import java.util.List;
import com.alibaba.fastjson.JSON;
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
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        canalMessage.setTimestamp(df.toString());
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
