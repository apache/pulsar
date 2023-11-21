package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class AtomicIntegerFlagsTest {

    @Test
    public void testEnablingAndDisablingFlags() {
        AtomicIntegerFlags flags = new AtomicIntegerFlags();
        // should return true when value is changed
        assertTrue(flags.changeFlag(5, true));
        assertTrue(flags.changeFlag(12, true));
        // should not return true when value isn't changed
        assertFalse(flags.changeFlag(5, true));
        for (int i = 0; i < 31; i++) {
            if (i == 5 || i == 12) {
                assertTrue(flags.getFlag(i));
            } else {
                assertFalse(flags.getFlag(i));
            }
        }
        assertTrue(flags.changeFlag(5, false));
        for (int i = 0; i < 31; i++) {
            if (i == 12) {
                assertTrue(flags.getFlag(i));
            } else {
                assertFalse(flags.getFlag(i));
            }
        }
    }

}