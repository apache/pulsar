package org.apache.pulsar.client.util;

import org.apache.pulsar.client.util.MathUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

public class MathUtilsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Rule public final Timeout globalTimeout = new Timeout(10000);

  // Test written by Diffblue Cover.
  @Test
  public void signSafeModInputNegativeNegativeOutputNegative() {

    // Act and Assert result
    Assert.assertEquals(-524_289, MathUtils.signSafeMod(-524_289L, -524_288));
  }

  // Test written by Diffblue Cover.
  @Test
  public void signSafeModInputPositivePositiveOutputZero() {

    // Act and Assert result
    Assert.assertEquals(0, MathUtils.signSafeMod(4L, 2));
  }
}
