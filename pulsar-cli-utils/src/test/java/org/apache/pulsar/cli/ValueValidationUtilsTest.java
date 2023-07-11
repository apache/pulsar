package org.apache.pulsar.cli;

import static org.testng.Assert.assertThrows;
import com.beust.jcommander.ParameterException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ValueValidationUtilsTest {

    @DataProvider(name = "maxValueCheckData")
    public static Object[][] maxValueCheckData() {
        return new Object[][]{
                {"param1", 11L, 10L},  // value > max
                {"param2", 10L, 10L},  // value = max
                {"param3", 9L, 10L}    // value < max
        };
    }

    @Test(dataProvider = "maxValueCheckData")
    public void maxValueCheckTest(String param, long value, long max) {
        if (value > max) {
            assertThrows(ParameterException.class, () -> ValueValidationUtils.maxValueCheck(param, value, max));
        } else {
            ValueValidationUtils.maxValueCheck(param, value, max);  // should not throw exception
        }
    }

    @DataProvider(name = "positiveCheckData")
    public static Object[][] positiveCheckData() {
        return new Object[][]{
                {"param1", 0L},     // value = 0
                {"param2", -1L},    // value < 0
                {"param3", 1L},     // value > 0
                {"param4", 0},      // value = 0
                {"param5", -1},     // value < 0
                {"param6", 1}       // value > 0
        };
    }

    @Test(dataProvider = "positiveCheckData")
    public void positiveCheckTest(String param, long value) {
        if (value <= 0) {
            assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck(param, value));
        } else {
            ValueValidationUtils.positiveCheck(param, value);  // should not throw exception
        }
    }

    @DataProvider(name = "emptyCheckData")
    public static Object[][] emptyCheckData() {
        return new Object[][]{
                {"param1", ""},         // value is empty string
                {"param2", null},       // value is null
                {"param3", "nonEmpty"}  // value is not empty
        };
    }

    @Test(dataProvider = "emptyCheckData")
    public void emptyCheckTest(String param, String value) {
        if (value == null || value.isEmpty()) {
            assertThrows(ParameterException.class, () -> ValueValidationUtils.emptyCheck(param, value));
        } else {
            ValueValidationUtils.emptyCheck(param, value);  // should not throw exception
        }
    }

    @DataProvider(name = "minValueCheckData")
    public static Object[][] minValueCheckData() {
        return new Object[][]{
                {"param1", 9L, 10L},   // value < min
                {"param2", 10L, 10L},  // value = min
                {"param3", 11L, 10L}   // value > min
        };
    }

    @Test(dataProvider = "minValueCheckData")
    public void minValueCheckTest(String param, long value, long min) {
        if (value < min) {
            assertThrows(ParameterException.class, () -> ValueValidationUtils.minValueCheck(param, value, min));
        } else {
            ValueValidationUtils.minValueCheck(param, value, min);  // should not throw exception
        }
    }

    @DataProvider(name = "positiveCheckIntData")
    public static Object[][] positiveCheckIntData() {
        return new Object[][]{
                {"param1", 0},      // value = 0
                {"param2", -1},     // value < 0
                {"param3", 1}       // value > 0
        };
    }

    @Test(dataProvider = "positiveCheckIntData")
    public void positiveCheckIntTest(String param, int value) {
        if (value <= 0) {
            assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck(param, value));
        } else {
            ValueValidationUtils.positiveCheck(param, value);  // should not throw exception
        }
    }
}
