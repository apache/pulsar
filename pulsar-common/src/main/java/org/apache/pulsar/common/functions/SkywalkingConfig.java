package org.apache.pulsar.common.functions;

import lombok.*;

@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder=true)
public class SkywalkingConfig {

    private String skywalkingAgentJarPath;

    private String skywalkingAgentConfPath;

    private String skywalkingBackendService;

}
