package org.apache.pulsar.websocket.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Represent result of request to check if we've reached end of topic.
 */
@Data
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EndOfTopicResponse {
    // If reach end of topic.
    public boolean endOfTopic;
}
