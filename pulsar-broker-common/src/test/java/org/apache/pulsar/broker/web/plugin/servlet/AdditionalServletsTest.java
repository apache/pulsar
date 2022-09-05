package org.apache.pulsar.broker.web.plugin.servlet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class AdditionalServletsTest {


    @Test
    public void testEmptyStringAsExtractionDirectory() throws IOException {
        Properties p = new Properties();
        p.put("narExtractionDirectory", "");
        p.put("additionalServlets", "AS1,AS2");
        p.put("additionalServletDirectory", "/additionalServletDirectory");

        PulsarConfiguration config = mock(PulsarConfiguration.class);
        Mockito.when(config.getProperties()).thenReturn(p);

        AdditionalServletMetadata asm1 = additionalServletMetadata(1);
        AdditionalServletMetadata asm2 = additionalServletMetadata(2);

        AdditionalServletDefinitions definitions = new AdditionalServletDefinitions();
        definitions.servlets().put("AS1", asm1);
        definitions.servlets().put("AS2", asm2);

        AdditionalServletWithClassLoader as1 = mock(AdditionalServletWithClassLoader.class);
        AdditionalServletWithClassLoader as2 = mock(AdditionalServletWithClassLoader.class);

        String originalTmpDirectory = System.getProperty("java.io.tmpdir");
        try (MockedStatic<AdditionalServletUtils> utils = mockStatic(AdditionalServletUtils.class)) {
            String tmpDirectory = "/my/tmp/directory";
            System.setProperty("java.io.tmpdir", tmpDirectory);
            utils.when(() -> AdditionalServletUtils.searchForServlets(
                    "/additionalServletDirectory", tmpDirectory)).thenReturn(definitions);
            utils.when(() -> AdditionalServletUtils.load(asm1, tmpDirectory)).thenReturn(as1);
            utils.when(() -> AdditionalServletUtils.load(asm2, tmpDirectory)).thenReturn(as2);

            AdditionalServlets servlets = AdditionalServlets.load(config);

            Assert.assertEquals(servlets.getServlets().get("AS1"), as1);
            Assert.assertEquals(servlets.getServlets().get("AS2"), as2);
        } finally {
            System.setProperty("java.io.tmpdir", originalTmpDirectory);
        }
    }

    private AdditionalServletMetadata additionalServletMetadata(int index) {
        AdditionalServletMetadata as = new AdditionalServletMetadata();
        as.setArchivePath(Paths.get("/additionalServletDirectory/" + index));
        as.setDefinition(new AdditionalServletDefinition());
        as.getDefinition().setName("as" + index);
        as.getDefinition().setAdditionalServletClass("com.example.AS" + index);
        as.getDefinition().setDescription("Additional Servlet " +index);
        return as;
    }
}
