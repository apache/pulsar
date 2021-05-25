package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({"receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal", "userExceptionsTotal",
        "avgProcessLatency", "1min", "lastInvocation", "instances"})
public abstract class FunctionStatsMixIn {
    @JsonProperty("1min")
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin;

    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonPropertyOrder({ "instanceId", "metrics" })
    public abstract static class FunctionInstanceStatsMixIn {

        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
                "userExceptionsTotal", "avgProcessLatency" })
        public abstract static class FunctionInstanceStatsDataBaseMixIn {

        }

        @JsonInclude(JsonInclude.Include.ALWAYS)
        @JsonPropertyOrder({ "receivedTotal", "processedSuccessfullyTotal", "systemExceptionsTotal",
                "userExceptionsTotal", "avgProcessLatency", "1min", "lastInvocation", "userMetrics" })
        public abstract static class FunctionInstanceStatsDataMixIn extends FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase {
            @JsonProperty("1min")
            public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase oneMin;
        }

    }
}
