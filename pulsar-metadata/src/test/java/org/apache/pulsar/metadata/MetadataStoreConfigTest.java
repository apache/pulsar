package org.apache.pulsar.metadata;

import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.junit.Assert;
import org.junit.Test;


public class MetadataStoreConfigTest {
    @Test
    public void testGeneCallerStackNameWhenMetadataStoreNameNotSet() {
        MetadataStoreConfig build = MetadataStoreConfig.builder().build();
        Assert.assertNotNull(build.getMetadataStoreName());

        System.out.println(build);
    }
}
