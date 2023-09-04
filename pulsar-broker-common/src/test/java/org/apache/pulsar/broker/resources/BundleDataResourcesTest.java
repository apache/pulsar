package org.apache.pulsar.broker.resources;

import static org.apache.pulsar.broker.resources.BaseResources.joinPath;
import static org.apache.pulsar.broker.resources.BundleDataResources.BUNDLE_DATA_BASE_PATH;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BundleDataResourcesTest {
    private MetadataStore configurationStore;
    private MetadataStore localStore;
    private BundleDataResources bundleDataResources;

    @BeforeMethod
    public void setup() {
        localStore = mock(MetadataStore.class);
        configurationStore = mock(MetadataStore.class);
        bundleDataResources = new BundleDataResources(localStore, 30);
    }

    /**
     *  Test that the bundle-data node is deleted from the local stores.
     */
    @Test
    public void testDeleteBundleDataAsync() {
        NamespaceName ns = NamespaceName.get("my-tenant/my-ns");
        String namespaceBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, ns.toString());
        bundleDataResources.deleteBundleDataAsync(ns);

        String tenant="my-tenant";
        String tenantBundlePath = joinPath(BUNDLE_DATA_BASE_PATH, tenant);
        bundleDataResources.deleteBundleDataTenantAsync(tenant);

        verify(localStore).deleteRecursive(namespaceBundlePath);
        verify(localStore).deleteRecursive(tenantBundlePath);

        assertThrows(()-> verify(configurationStore).deleteRecursive(namespaceBundlePath));
        assertThrows(()-> verify(configurationStore).deleteRecursive(tenantBundlePath));
    }
}
