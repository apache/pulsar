package org.apache.pulsar.common.policies.data;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicHashPositions {
    private String namespace;
    private String bundle;
    private Map<String, Long> topicHashPositions;
}
