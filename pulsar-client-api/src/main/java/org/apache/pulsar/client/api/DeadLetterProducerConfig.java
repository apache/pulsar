package org.apache.pulsar.client.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration used to create a producer that will send messages to
 * the dead letter topic and retry topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeadLetterProducerConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // refer to default values in ProducerConfigurationData
    public static final int DEFAULT_BATCHING_MAX_MESSAGES = 1000;
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 0;

    /**
     * @see ProducerBuilder#blockIfQueueFull(boolean)
     */
    private boolean blockIfQueueFull = false;

    /**
     * @see ProducerBuilder#batchingMaxMessages(int)
     */
    private int maxPendingMessages = DEFAULT_MAX_PENDING_MESSAGES;

    /**
     * @see ProducerBuilder#enableBatching(boolean)
     * default is false to keep the same behavior as before
     * while the default value in ProducerConfigurationData is true
     */
    private boolean batchingEnabled = false;

    /**
     * @see ProducerBuilder#batchingMaxMessages(int)
     */
    private int batchingMaxMessages = DEFAULT_BATCHING_MAX_MESSAGES;

    /**
     * @see ProducerBuilder#batchingMaxBytes(int)
     */
    private int batchingMaxBytes = 128 * 1024;

    /**
     * @see ProducerBuilder#enableChunking(boolean)
     * default is true to keep the same behavior as before
     * while the default value in ProducerConfigurationData is false
     */
    private boolean chunkingEnabled = true;

    /**
     * @see ProducerBuilder#chunkMaxMessageSize(int) 
     */
    private int chunkMaxMessageSize = -1;

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("blockIfQueueFull", blockIfQueueFull);
        map.put("maxPendingMessages", maxPendingMessages);
        map.put("batchingEnabled", batchingEnabled);
        map.put("batchingMaxMessages", batchingMaxMessages);
        map.put("batchingMaxBytes", batchingMaxBytes);
        map.put("chunkingEnabled", chunkingEnabled);
        map.put("chunkMaxMessageSize", chunkMaxMessageSize);
        return map;
    }
}
