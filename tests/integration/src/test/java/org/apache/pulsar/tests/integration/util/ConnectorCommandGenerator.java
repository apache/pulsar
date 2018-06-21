package org.apache.pulsar.tests.integration.util;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.io.cassandra.CassandraStringSink;
import org.apache.pulsar.io.kafka.KafkaStringSink;
import org.apache.pulsar.io.kafka.KafkaStringSource;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class ConnectorCommandGenerator {
    public enum Type {
        SOURCE,
        SINK
    }
    private Type type;
    public enum Connector {
        CASSANDRA,
        KAFKA,
        TWITTER,
        WORD
    }
    private static final Map<Connector, String> sinkToClassName;
    static {
        sinkToClassName = new HashMap<>();
        sinkToClassName.put(Connector.CASSANDRA, CassandraStringSink.class.getName());
        sinkToClassName.put(Connector.KAFKA, KafkaStringSink.class.getName());
    }
    private static final Map<Connector, String> sourceToClassName;
    static {
        sourceToClassName = new HashMap<>();
        sourceToClassName.put(Connector.KAFKA, KafkaStringSource.class.getName());
    }
    private static final Map<Connector, String> connectorToJarName;
    static {
        connectorToJarName = new HashMap<>();
        connectorToJarName.put(Connector.CASSANDRA, "/pulsar-container-integration-tests/core/build/libs/core-0.0.1.jar");
        connectorToJarName.put(Connector.KAFKA, "/pulsar-container-integration-tests/core/build/libs/core-0.0.1.jar");
        connectorToJarName.put(Connector.TWITTER, "/pulsar-container-integration-tests/core/build/libs/core-0.0.1.jar");
        connectorToJarName.put(Connector.WORD, "/pulsar-container-integration-tests/core/build/libs/core-0.0.1.jar");
    }

    private Connector connector;
    private String sourceTopic;
    private Map<String, String> customSereSourceTopics;
    private String sinkTopic;
    private Integer parallelism;
    private String adminUrl;
    private String name;
    private Map<String, Object> config;
    private String configFile;


    public static ConnectorCommandGenerator createCassandraSink(String sourceTopic,
                                                                String roots, String keySpace,
                                                                String columnFamily, String keyName,
                                                                String columnName) {
        ConnectorCommandGenerator generator = new ConnectorCommandGenerator();
        generator.setType(Type.SINK);
        generator.setConnector(Connector.CASSANDRA);
        generator.setSourceTopic(sourceTopic);
        generator.setParallelism(1);
        generator.setName("cassandrasink");
        Map<String, Object> sinkConfig = new HashMap<>();
        sinkConfig.put("roots", roots);
        sinkConfig.put("keyspace", keySpace);
        sinkConfig.put("keyname", keyName);
        sinkConfig.put("columnFamily", columnFamily);
        sinkConfig.put("columnName", columnName);
        generator.setConfig(sinkConfig);
        return generator;
    }

    public static ConnectorCommandGenerator createKafkaSink(String pulsarSourceTopic,
                                                            String bootstrapServers,
                                                            String acks,
                                                            Long batchSize,
                                                            Long maxRequestSize,
                                                            String topic) {
        ConnectorCommandGenerator generator = new ConnectorCommandGenerator();
        generator.setType(Type.SINK);
        generator.setConnector(Connector.KAFKA);
        generator.setSourceTopic(pulsarSourceTopic);
        generator.setParallelism(1);
        generator.setName("kafkasink");
        Map<String, Object> sinkConfig = new HashMap<>();
        sinkConfig.put("bootstrapServers", bootstrapServers);
        sinkConfig.put("acks", acks);
        sinkConfig.put("batchSize", batchSize);
        sinkConfig.put("maxRequestSize", maxRequestSize);
        sinkConfig.put("topic", topic);
        generator.setConfig(sinkConfig);
        return generator;
    }

    public static ConnectorCommandGenerator createKafkaSource(String pulsarSinkTopic,
                                                            String bootstrapServers,
                                                            String groupId,
                                                            Long fetchMinBytes,
                                                            Long autoCommitIntervalMs,
                                                            Long sessionTimeoutMs,
                                                            String valueDeserializerClass,
                                                            String topic) {
        ConnectorCommandGenerator generator = new ConnectorCommandGenerator();
        generator.setType(Type.SOURCE);
        generator.setConnector(Connector.KAFKA);
        generator.setSinkTopic(pulsarSinkTopic);
        generator.setParallelism(1);
        generator.setName("kafkasource");
        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put("bootstrapServers", bootstrapServers);
        sourceConfig.put("groupId", groupId);
        sourceConfig.put("fetchMinBytes", fetchMinBytes);
        sourceConfig.put("autoCommitIntervalMs", autoCommitIntervalMs);
        sourceConfig.put("sessionTimeoutMs", sessionTimeoutMs);
        sourceConfig.put("topic", topic);
        sourceConfig.put("valueDeserializationClass", valueDeserializerClass);
        generator.setConfig(sourceConfig);
        return generator;
    }

    public static ConnectorCommandGenerator createTwitterSource(String pulsarSinkTopic,
                                                              String secretsFile) {
        ConnectorCommandGenerator generator = new ConnectorCommandGenerator();
        generator.setType(Type.SOURCE);
        generator.setConnector(Connector.TWITTER);
        generator.setSinkTopic(pulsarSinkTopic);
        generator.setParallelism(1);
        generator.setName("twittersource");
        generator.setConfigFile(secretsFile);
        return generator;
    }

    public static ConnectorCommandGenerator createWordSource(String pulsarSinkTopic) {
        ConnectorCommandGenerator generator = new ConnectorCommandGenerator();
        generator.setType(Type.SOURCE);
        generator.setConnector(Connector.WORD);
        generator.setSinkTopic(pulsarSinkTopic);
        generator.setParallelism(1);
        generator.setName("wordsource");
        return generator;
    }

    public void createAdminUrl(String workerHost, int port) {
        adminUrl = "http://" + workerHost + ":" + port;
    }

    public String generateCreateFunctionCommand() {
        StringBuilder commandBuilder = new StringBuilder("PULSAR_MEM=-Xmx1024m ");
        if (adminUrl == null) {
            commandBuilder.append("/pulsar/bin/pulsar-admin");
        } else {
            commandBuilder.append("/pulsar/bin/pulsar-admin");
            commandBuilder.append(" --admin-url ");
            commandBuilder.append(adminUrl);
        }
        if (type == Type.SINK) {
            commandBuilder.append(" sink create");
            commandBuilder.append(" --className " + sinkToClassName.get(connector));
            if (config != null) {
                commandBuilder.append(" --sinkConfig \'" + new Gson().toJson(config) + "\'");
            }
            if (configFile != null) {
                commandBuilder.append(" --sinkConfigFile " + configFile);
            }
        } else {
            commandBuilder.append(" source create");
            commandBuilder.append(" --className " + sourceToClassName.get(connector));
            if (config != null) {
                commandBuilder.append(" --sourceConfig \'" + new Gson().toJson(config) + "\'");
            }
            if (configFile != null) {
                commandBuilder.append(" --sourceConfigFile " + configFile);
            }
        }
        commandBuilder.append(" --tenant " + getTenant());
        commandBuilder.append(" --namespace " + getNamespace());
        commandBuilder.append(" --name " + getName());
        if (sourceTopic != null) {
            commandBuilder.append(" --inputs " + sourceTopic);
        }
        if (customSereSourceTopics != null && !customSereSourceTopics.isEmpty()) {
            commandBuilder.append(" --customSerdeInputs \'" + new Gson().toJson(customSereSourceTopics) + "\'");
        }
        if (parallelism != null) {
            commandBuilder.append(" --parallelism " + parallelism);
        }
        if (sinkTopic != null) {
            commandBuilder.append(" --destinationTopicName " + sinkTopic);
        }

        commandBuilder.append(" --jar " + connectorToJarName.get(connector));
        return commandBuilder.toString();
    }

    public String genereateDeleteCassandraSinkCommand() {
        StringBuilder commandBuilder = new StringBuilder("/pulsar/bin/pulsar-admin sink delete");
        commandBuilder.append(" --tenant public ");
        commandBuilder.append(" --namespace default");
        commandBuilder.append(" --name " + name);
        return commandBuilder.toString();
    }

    public String getTenant() {
        return "public";
    }

    public String getNamespace() {
        return "default";
    }

    public String getName() {
        return name;
    }
}
