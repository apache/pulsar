package org.apache.pulsar;

// 1. The import order is wrong
import org.testng.Assert;
import org.testng.annotations.Test;

public class SimpleTest {
  @Test // 2. The indents and number blanks are wrong
  public void test() {
    Assert.assertEquals(1 + 2, 3);
  }
}
