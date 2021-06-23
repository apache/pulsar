package org.apache.pulsar.client.impl.conf;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ProducerConfigurationDataTest {
    private static final String PRODUCER_NAME = "test-producer";

    private ProducerConfigurationData confData;

    @BeforeTest
    public void setUp() {
        confData = new ProducerConfigurationData();
        confData.setProducerName(PRODUCER_NAME);
    }

    @AfterTest
    public void ensurePreserveOtherProperties() {
        assertEquals(PRODUCER_NAME, confData.getProducerName());
    }

    @Test
    public void testLoadBatcherBuilderDefaultCase() {
        assertEquals(BatcherBuilder.DEFAULT, confData.getBatcherBuilder());
    }

    @Test
    public void testLoadChangeBatcherBuilderTypeToKeyBased() {
        confData.setBatcherBuilderType(ProducerConfigurationData.BatcherBuilderType.Default);

        Map<String, Object> config = new HashMap<>();
        config.put("batcherBuilderType", "Key_Based");

        confData = ConfigurationDataUtils.loadData(config, confData, ProducerConfigurationData.class);

        assertEquals(BatcherBuilder.KEY_BASED, confData.getBatcherBuilder());
    }

    @Test
    public void testLoadChangeBatcherBuilderTypeToDefault() {
        confData.setBatcherBuilderType(ProducerConfigurationData.BatcherBuilderType.Key_Based);

        Map<String, Object> config = new HashMap<>();
        config.put("batcherBuilderType", "Default");

        confData = ConfigurationDataUtils.loadData(config, confData, ProducerConfigurationData.class);

        assertEquals(BatcherBuilder.DEFAULT, confData.getBatcherBuilder());
    }

    @Test
    public void testLoadBatcherBuilderTypeUnknown() {
        Map<String, Object> config = new HashMap<>();
        config.put("batcherBuilderType", "UNKNOWN");

        assertThrows(RuntimeException.class, () -> ConfigurationDataUtils.loadData(config, confData, ProducerConfigurationData.class));
    }
}