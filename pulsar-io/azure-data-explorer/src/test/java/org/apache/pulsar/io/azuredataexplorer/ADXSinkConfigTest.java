package org.apache.pulsar.io.azuredataexplorer;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

public class ADXSinkConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        ADXSinkConfig config = ADXSinkConfig.load(path);
        assertNotNull(config);
        assertEquals(config.getClusterUrl(), "https://somecluster.eastus.kusto.windows.net");
        assertEquals(config.getDatabase(), "somedb");
        assertEquals(config.getTable(), "tableName");
        assertEquals(config.getAppId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getAppKey(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getTenantId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getManagedIdentityId(), "xxxx-some-id-xxxx OR empty string");
        assertEquals(config.getMappingRefName(), "mapping ref name");
        assertEquals(config.getMappingRefType(), "CSV");
        assertFalse(config.isFlushImmediately());
        assertEquals(config.getBatchSize(), 100);
        assertFalse(config.isManagedIngestion());
        assertEquals(config.getBatchTimeMs(), 10000);
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("clusterUrl", "https://somecluster.eastus.kusto.windows.net");
        map.put("database", "somedb");
        map.put("table", "tableName");
        map.put("appId", "xxxx-xxxx-xxxx-xxxx");
        map.put("appKey", "xxxx-xxxx-xxxx-xxxx");
        map.put("tenantId", "xxxx-xxxx-xxxx-xxxx");
        //map.put("managedIdentityId", "xxxx-some-id-xxxx OR empty string");
        map.put("mappingRefName", "mapping ref name");
        map.put("mappingRefType", "CSV");
        map.put("flushImmediately", false);
        map.put("managedIngestion", false);
        map.put("batchSize", 100);
        map.put("batchTimeMs", 10000);

        ADXSinkConfig config = ADXSinkConfig.load(map);
        assertNotNull(config);
        assertEquals(config.getClusterUrl(), "https://somecluster.eastus.kusto.windows.net");
        assertEquals(config.getDatabase(), "somedb");
        assertEquals(config.getTable(), "tableName");
        assertEquals(config.getAppId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getAppKey(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getTenantId(), "xxxx-xxxx-xxxx-xxxx");
        assertEquals(config.getManagedIdentityId(), "xxxx-some-id-xxxx OR empty string");
        assertEquals(config.getMappingRefName(), "mapping ref name");
        assertEquals(config.getMappingRefType(), "CSV");
        assertFalse(config.isFlushImmediately());
        assertEquals(config.getBatchSize(), 100);
        assertFalse(config.isManagedIngestion());
        assertEquals(config.getBatchTimeMs(), 10000);
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
