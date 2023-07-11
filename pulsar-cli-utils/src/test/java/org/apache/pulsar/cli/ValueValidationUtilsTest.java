package org.apache.pulsar.cli;

import static org.testng.Assert.assertThrows;
import com.beust.jcommander.ParameterException;
import org.testng.annotations.Test;

public class ValueValidationUtilsTest {

    @Test
    public void testMaxValueCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.maxValueCheck("param1", 11L, 10L));
        ValueValidationUtils.maxValueCheck("param2", 10L, 10L);
        ValueValidationUtils.maxValueCheck("param3", 9L, 10L);
    }

    @Test
    public void testPositiveCheck() {
        // Long
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param1", 0L));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param2", -1L));
        ValueValidationUtils.positiveCheck("param3", 1L);

        // Integer
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param4", 0));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param5", -1));
        ValueValidationUtils.positiveCheck("param6", 1);
    }

    @Test
    public void testEmptyCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.emptyCheck("param1", ""));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.emptyCheck("param2", null));
        ValueValidationUtils.emptyCheck("param3", "nonEmpty");
    }

    @Test
    public void testMinValueCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.minValueCheck("param1", 9L, 10L));
        ValueValidationUtils.minValueCheck("param2", 10L, 10L);
        ValueValidationUtils.minValueCheck("param3", 11L, 10L);
    }

    @Test
    public void testPositiveCheckInt() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param1", 0));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param2", -1));
        ValueValidationUtils.positiveCheck("param3", 1);
    }
}
