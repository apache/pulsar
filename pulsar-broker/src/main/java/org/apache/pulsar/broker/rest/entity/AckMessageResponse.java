package org.apache.pulsar.broker.rest.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AckMessageResponse {
    List<RestAckPosition> messageIds;
}
