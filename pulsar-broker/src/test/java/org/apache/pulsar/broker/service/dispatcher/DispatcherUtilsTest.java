package org.apache.pulsar.broker.service.dispatcher;

import org.apache.pulsar.common.nar.NarClassLoader;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

@PrepareForTest({
        DispatcherUtils.class, NarClassLoader.class
})
@PowerMockIgnore({"org.apache.logging.log4j.*"})
public class DispatcherUtilsTest {

    // Necessary to make PowerMockito.mockStatic work with TestNG.
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testLoadDispatcher() throws Exception {
        String dispatcherClassName = "org.apache.pulsar.my.smart.dispatcher";
        String narPath = "/path/to/dispatcher/nar";

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        Class dispatcherClass = MockCustomizedDispatcher.class;
        when(mockLoader.loadClass(eq(dispatcherClassName)))
                .thenReturn(dispatcherClass);

        PowerMockito.mockStatic(NarClassLoader.class);
        PowerMockito.when(NarClassLoader.getFromArchive(
                any(File.class),
                any(Set.class)
        )).thenReturn(mockLoader);

        DispatcherUtils.init(narPath);
        Dispatcher returnedDispatcher = DispatcherUtils.load(dispatcherClassName);

        assertTrue(returnedDispatcher instanceof MockCustomizedDispatcher);
    }

    @Test
    public void testExceptionIfNarPathNotSpecified() throws Exception {
        String dispatcherClassName = "org.apache.pulsar.my.smart.dispatcher";

        NarClassLoader mockLoader = mock(NarClassLoader.class);
        Class dispatcherClass = MockCustomizedDispatcher.class;
        when(mockLoader.loadClass(eq(dispatcherClassName)))
                .thenReturn(dispatcherClass);

        PowerMockito.mockStatic(NarClassLoader.class);
        PowerMockito.when(NarClassLoader.getFromArchive(
                any(File.class),
                any(Set.class)
        )).thenReturn(mockLoader);

        DispatcherUtils.init(null);
        try {
            DispatcherUtils.load(dispatcherClassName);
            fail("Should not reach here");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }
}
