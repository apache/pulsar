package org.apache.pulsar.client.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Configuration used to create a producer that will send messages to
 * the dead letter topic and retry topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeadLetterProducerConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * @see ProducerBuilder#blockIfQueueFull(boolean)
     */
    private boolean blockIfQueueFull = false;


}
