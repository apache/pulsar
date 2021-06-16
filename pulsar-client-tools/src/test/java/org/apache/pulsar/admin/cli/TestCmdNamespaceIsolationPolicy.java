package org.apache.pulsar.admin.cli;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.testng.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;


public class TestCmdNamespaceIsolationPolicy {
    @Test
    public void testValidateListMethodToReturnNonNullStringList() {
        List<String> mockList = Lists.newArrayList("", "1", "", "", "1", "2", "3", "4", "", "", "", "1", "");
        List<String> resultList = Lists.newArrayList("1", "1", "2", "3", "4", "1");
        CmdNamespaceIsolationPolicy cmdNamespaceIsolationPolicy = new CmdNamespaceIsolationPolicy(() -> null);
        Class<? extends CmdNamespaceIsolationPolicy> klass = cmdNamespaceIsolationPolicy.getClass();
        Arrays.stream(klass.getDeclaredMethods())
                .filter((innerMethod) -> innerMethod.getName().contains("validateList"))
                .findFirst().ifPresent(innerMethod -> {
            try {
                innerMethod.setAccessible(true);
                List<String> calculatedList = (List<String>) innerMethod.invoke(cmdNamespaceIsolationPolicy, mockList);
                Assert.assertEquals(calculatedList.size(), resultList.size());
                for (int i = 0; i < resultList.size(); i++)
                    Assert.assertEquals(resultList.get(i), calculatedList.get(i));
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        });
    }
}
