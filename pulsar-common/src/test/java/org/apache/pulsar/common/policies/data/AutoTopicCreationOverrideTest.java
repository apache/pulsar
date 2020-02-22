package org.apache.pulsar.common.policies.data;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AutoTopicCreationOverrideTest {

    @Test
    public void testValidOverrideNonPartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.NON_PARTITIONED.toString(), null);
        assertTrue(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testValidOverridePartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), 2);
        assertTrue(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testInvalidTopicType() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, "aaa", null);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsTooLow() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), 0);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsNotSet() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.PARTITIONED.toString(), null);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }

    @Test
    public void testNumPartitionsOnNonPartitioned() {
        AutoTopicCreationOverride override = new AutoTopicCreationOverride(true, TopicType.NON_PARTITIONED.toString(), 2);
        assertFalse(AutoTopicCreationOverride.isValidOverride(override));
    }
}
