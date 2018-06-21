package org.apache.pulsar.tests.integration.util;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
public class LocalRunCommandGenerator {
    public enum Runtime {
        JAVA,
        PYTHON,
    };
    private String functionName;
    private String tenant;
    private String namespace;
    private String functionClassName;
    private String sourceTopic;
    private Map<String, String> customSereSourceTopics;
    private String sinkTopic;
    private String outputSerDe;
    private String processingGuarantees;
    private Runtime runtime;
    private Integer parallelism;
    private String brokerUrl;
    private Map<String, String> userConfig = new HashMap<>();
    private static final String JAVAJAR = "/pulsar-container-integration-tests/core/build/libs/core-0.0.1.jar";
    private static final String PYTHONBASE = "/pulsar-container-integration-tests/core/src/main/python/";

    public static LocalRunCommandGenerator createDefaultGenerator(String sourceTopic, String functionClassName) {
        LocalRunCommandGenerator generator = new LocalRunCommandGenerator();
        generator.setSourceTopic(sourceTopic);
        generator.setFunctionClassName(functionClassName);
        generator.setRuntime(Runtime.JAVA);
        return generator;
    }

    public static LocalRunCommandGenerator createDefaultGenerator(Map<String, String> customSereSourceTopics, String functionClassName) {
        LocalRunCommandGenerator generator = new LocalRunCommandGenerator();
        generator.setCustomSereSourceTopics(customSereSourceTopics);
        generator.setFunctionClassName(functionClassName);
        generator.setRuntime(Runtime.JAVA);
        return generator;
    }

    public static LocalRunCommandGenerator createDefaultGenerator(String tenant, String namespace, String functionName) {
        LocalRunCommandGenerator generator = new LocalRunCommandGenerator();
        generator.setTenant(tenant);
        generator.setNamespace(namespace);
        generator.setFunctionName(functionName);
        generator.setRuntime(Runtime.JAVA);
        return generator;
    }

    public void createBrokerUrl(String host, int port) {
        brokerUrl = "pulsar://" + host + ":" + port;
    }

    public List<String> generateLocalRunCommand() {
        return generateLocalRunCommand(null);
    }

    public List<String> generateLocalRunCommand(String codeFile) {
        List<String> retVal = new LinkedList<>();
        retVal.add("PULSAR_MEM=-Xmx1024m");
        retVal.add("/pulsar/bin/pulsar-admin");
        retVal.add("functions");
        retVal.add("localrun");
        if (brokerUrl != null) {
            retVal.add("--brokerServiceUrl");
            retVal.add(brokerUrl);
        }
        if (tenant != null) {
            retVal.add("--tenant");
            retVal.add(tenant);
        }
        if (namespace != null) {
            retVal.add("--namespace");
            retVal.add(namespace);
        }
        if (functionName != null) {
            retVal.add("--name");
            retVal.add(functionName);
        }
        retVal.add("--className");
        retVal.add(functionClassName);
        if (sourceTopic != null) {
            retVal.add("--inputs");
            retVal.add(sourceTopic);
        }
        if (customSereSourceTopics != null && !customSereSourceTopics.isEmpty()) {
            retVal.add("--customSerdeInputs");
            retVal.add("\'" + new Gson().toJson(customSereSourceTopics) + "\'");
        }
        if (sinkTopic != null) {
            retVal.add("--output");
            retVal.add(sinkTopic);
        }
        if (outputSerDe != null) {
            retVal.add("--outputSerdeClassName");
            retVal.add(outputSerDe);
        }
        if (processingGuarantees != null) {
            retVal.add("--processingGuarantees");
            retVal.add(processingGuarantees);
        }
        if (!userConfig.isEmpty()) {
            retVal.add("--userConfig");
            retVal.add("\'" + new Gson().toJson(userConfig) + "\'");
        }
        if (parallelism != null) {
            retVal.add("--parallelism");
            retVal.add(Integer.toString(parallelism));
        }

        if (runtime == Runtime.JAVA) {
            retVal.add("--jar");
            retVal.add(JAVAJAR);
        } else {
            if (codeFile != null) {
                retVal.add("--py");
                retVal.add(PYTHONBASE + codeFile);
            } else {
                retVal.add("--py");
                retVal.add(PYTHONBASE);
            }
        }
        return retVal;
    }
}
