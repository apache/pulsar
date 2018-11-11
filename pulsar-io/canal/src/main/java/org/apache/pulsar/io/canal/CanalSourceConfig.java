package org.apache.pulsar.io.canal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class CanalSourceConfig implements Serializable{

    private String username;
    private String password;
    private String destination;
    private String singleHostname;
    private int singlePort;
    private Boolean cluster;
    private String zkServers;
    private int batchSize;

    public static CanalSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CanalSourceConfig.class);
    }


    public static CanalSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CanalSourceConfig.class);
    }
}
