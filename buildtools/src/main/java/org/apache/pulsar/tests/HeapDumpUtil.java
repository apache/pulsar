/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.tests;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;

public class HeapDumpUtil {
    private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

    // Utility method to get the HotSpotDiagnosticMXBean
    private static HotSpotDiagnosticMXBean getHotSpotDiagnosticMXBean() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            return ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Dump the heap of the JVM.
     *
     * @param file        the system-dependent filename
     * @param liveObjects if true dump only live objects i.e. objects that are reachable from others
     */
    public static void dumpHeap(File file, boolean liveObjects) {
        try {
            HotSpotDiagnosticMXBean hotspotMBean = getHotSpotDiagnosticMXBean();
            hotspotMBean.dumpHeap(file.getAbsolutePath(), liveObjects);
        } catch (Exception e) {
            throw new RuntimeException("Error generating heap dump", e);
        }
    }
}
