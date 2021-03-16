package org.apache.bookkeeper.mledger.offload;

import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@PrepareForTest({OffloaderUtils.class})
@PowerMockIgnore({"org.apache.logging.log4j.*", "org.apache.pulsar.common.nar.*", "java.io.*"})
public class OffloadersCacheTest {

    // Necessary to make PowerMockito.mockStatic work with TestNG.
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testLoadsOnlyOnce() throws Exception {
        Offloaders expectedOffloaders = new Offloaders();

        PowerMockito.mockStatic(OffloaderUtils.class);
        PowerMockito.when(OffloaderUtils.searchForOffloaders(eq("./offloaders"), eq("/tmp")))
                .thenReturn(expectedOffloaders);

        OffloadersCache cache = new OffloadersCache();

        // Call a first time to load the offloader
        Offloaders offloaders1 = cache.getOrLoadOffloaders("./offloaders", "/tmp");

        assertSame(offloaders1, expectedOffloaders, "The offloaders should be the mocked one.");

        // Call a second time to get the stored offlaoder
        Offloaders offloaders2 = cache.getOrLoadOffloaders("./offloaders", "/tmp");

        assertSame(offloaders2, expectedOffloaders, "The offloaders should be the mocked one.");
    }

}
