package org.apache.pulsar.io.flume;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *  Flume general config.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class FlumeConfig {

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "the name of this agent")
    private String name;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify a config file (required if -z missing)")
    private String confFile;
    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "do not reload config file if changed")
    private Boolean noReloadConf;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify the ZooKeeper connection to use (required if -f missing)")
    private String zkConnString;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify the base path in ZooKeeper for agent configs")
    private String zkBasePath;

    public static FlumeConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), FlumeConfig.class);
    }


    public static FlumeConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), FlumeConfig.class);
    }
}
