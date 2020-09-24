package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Data
@AllArgsConstructor
@Setter
@Getter
@NoArgsConstructor
public class ProduceMessageResponse {

    List<ProduceMessageResult> results;

    @Setter
    @Getter
    public static class ProduceMessageResult {
        int partition;
        String messageId;
        int errorCode;
        String error;
    }
}
