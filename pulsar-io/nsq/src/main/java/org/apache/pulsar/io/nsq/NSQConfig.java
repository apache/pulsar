package org.apache.pulsar.io.nsq

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


import lombok.Data;
import lombok.experimental.Accessors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for the NSQ Connector.
 */
@Data
@Accessors(chain=true)
public class NSQSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;


    @FieldDoc(
        required= true,
        defaultValue = "",
        help = "The topic you wish to transport into pulsar"
    )
    private String topic;

    @FieldDoc(
        required= false,
        defaultValue = "pulsar-transport-<topic>",
        help = "The channel to use on the topic you want to transport"
    )
    private String channel;

    @FieldDoc(
        required= true,
        defaultValue = "",
        help = "A comma-separated list of nsqlookupd hosts to contact"
    )
    private String lookupds;

    public static NSQSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return applyDefaults(mapper.readValue(new File(yamlFile), NSQSourceConfig.class));
    }

    public static NSQSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return applyDefaults(mapper.readValue(new ObjectMapper().writeValueAsString(map), NSQSourceConfig.class));
    }

    private static NSQSourceConfig applyDefaults(NSQSourceConfig config) {
        if (config.channel == null) {
            config.channel=String.format("pulsar-transport-%s", config.topic);
        }
        return config;
    }


    public void validate() throws IllegalArgumentException {
        if (getChannel() == null) {
            setChannel(String.format("pulsar-transport-%s", getTopic()));
        }
        if (getTopic() == null || getLookupds() == null || getChannel() == null){
            throw new IllegalArgumentException("Required property not set.");
        }
    }

    public List<String> getLookupds(){
        if (StringUtils.isBlank(lookupds)){
            return Collections.emptyList();
        }

        List<String> out = new ArrayList<String> ();
        for (String s: StringUtils.split(lookupds, ",")) {
            out.add(StringUtils.trim(s));
        }

        if (CollectionUtils.isEmpty(out)){
            return Collections.emptyList();
        }
        return out;
    }
}
