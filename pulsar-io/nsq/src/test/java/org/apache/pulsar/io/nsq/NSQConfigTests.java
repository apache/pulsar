package org.apache.pulsar.io.nsq;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

public class NSQConfigTests {
    
    private NSQConfig config;

    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sourceConfig.yaml");
        config = NSQConfig.load(yamlFile.getAbsolutePath());
        assertNotNull(config);
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("topic", "xxx");
        map.put("channel", "xxx");
        map.put("lookupds", "xxx");
        
        config = NSQConfig.load(map);
        
        assertNotNull(config);
    }

    @Test
    public final void defaultValuesTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("topic", "xxx");
        map.put("lookupds", "xxx");
        
        config = NSQConfig.load(map);
        
        assertNotNull(config);
        assertEquals(config.getChannel(), "pulsar-transport-xxx");
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("topic", "xxx");
        map.put("channel", "xxx");
        map.put("lookupds", "xxx");
        
        config = NSQConfig.load(map);
        config.validate();
    }
    
    @Test(expectedExceptions = IllegalArgumentException.class, 
            expectedExceptionsMessageRegExp = "Required property not set.")
    public final void missingConsumerKeyValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        
        config = NSQConfig.load(map);
        config.validate();
    }
    
    @Test
    public final void getlookupdsTest() throws IOException {
        Map<String, Object> map = new HashMap<String, Object> ();
        map.put("lookupds", "one,two, three");
        config = NSQConfig.load(map);
        
        List<String> lookupds = config.getLookupds();
        assertNotNull(lookupds);
        assertEquals(lookupds.size(), 3);
        assertTrue(lookupds.contains("one"));
        assertTrue(lookupds.contains("two"));
        assertTrue(lookupds.contains("three"));
    }
    
    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }

}
