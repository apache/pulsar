package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertEquals;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LinuxInfoUtilsTest {

    /**
     * simulate reading contents in /sys/fs/cgroup/cpuset/cpuset.cpus to get the number of Cpus.
     */
    @Test
    public void testGetTotalCpuCount() {
        int totalCpuCount=0;
        String CPU_CPUSET_CPUS = "0-2,16,20-30";
        String[] ranges = CPU_CPUSET_CPUS.split(",");
        for (String range : ranges) {
            if (!range.contains("-")) {
                totalCpuCount++;
            } else {
                int dashIndex = range.indexOf('-');
                int left = Integer.valueOf(range.substring(0, dashIndex));
                int right = Integer.valueOf(range.substring(dashIndex + 1));
                totalCpuCount += right - left + 1;
            }
        }
        assertEquals(totalCpuCount, 15);
    }
}
