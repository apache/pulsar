package org.apache.pulsar.functions.api;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
public class FunctionConfig {

    public enum ProcessingGuarantees {
        ATLEAST_ONCE,
        ATMOST_ONCE,
        EFFECTIVELY_ONCE
    }

    public enum SubscriptionType {
        SHARED,
        EXCLUSIVE
    }

    public enum Runtime {
        JAVA,
        PYTHON
    }

    private String tenant;
    private String namespace;
    private String name;
    private String className;

    private Collection<String> inputs = new LinkedList<>();
    private Map<String, String> customSerdeInputs = new HashMap<>();

    private String output;
    private String outputSerdeClassName;

    private String logTopic;
    private ProcessingGuarantees processingGuarantees;
    private Map<String, String> userConfig = new HashMap<>();
    private SubscriptionType subscriptionType;
    private Runtime runtime;
    private boolean autoAck;
    private int parallelism;
    private String fqfn;
}
