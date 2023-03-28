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

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.testng.IExecutionListener;
import org.testng.ISuite;
import org.testng.ISuiteListener;

/**
 * A TestNG listener that dumps Jacoco coverage data to file using the Jacoco JMX interface.
 *
 * This ensures that coverage data is dumped even if the shutdown sequence of the Test JVM gets stuck. Coverage
 * data will be dumped every 2 minutes by default and once all test suites have been run.
 * Each test class runs in its own suite when run with maven-surefire-plugin.
 */
public class JacocoDumpListener implements ISuiteListener, IExecutionListener {
    private final MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    private final ObjectName jacocoObjectName;
    private final JacocoProxy jacocoProxy;
    private final boolean enabled;

    private long lastDumpTime;

    private static final long DUMP_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(120);

    public JacocoDumpListener() {
        try {
            jacocoObjectName = new ObjectName("org.jacoco:type=Runtime");
        } catch (MalformedObjectNameException e) {
            // this won't happen since the ObjectName is static and valid
            throw new RuntimeException(e);
        }
        enabled = checkEnabled();
        if (enabled) {
            jacocoProxy = MBeanServerInvocationHandler.newProxyInstance(platformMBeanServer, jacocoObjectName,
                    JacocoProxy.class, false);
        } else {
            jacocoProxy = null;
        }
        lastDumpTime = System.currentTimeMillis();
    }

    private boolean checkEnabled() {
        try {
            platformMBeanServer.getObjectInstance(jacocoObjectName);
        } catch (InstanceNotFoundException e) {
            // jacoco jmx is not enabled
            return false;
        }
        return true;
    }

    public void onFinish(ISuite suite) {
        // dump jacoco coverage data to file using the Jacoco JMX interface if more than DUMP_INTERVAL_MILLIS has passed
        // since the last dump
        if (enabled && System.currentTimeMillis() - lastDumpTime > DUMP_INTERVAL_MILLIS) {
            // dump jacoco coverage data to file using the Jacoco JMX interface
            triggerJacocoDump();
        }
    }
    @Override
    public void onExecutionFinish() {
        if (enabled) {
            // dump jacoco coverage data to file using the Jacoco JMX interface when all tests have finished
            triggerJacocoDump();
        }
    }

    private void triggerJacocoDump() {
        System.out.println("Dumping Jacoco coverage data to file...");
        long start = System.currentTimeMillis();
        jacocoProxy.dump(true);
        lastDumpTime = System.currentTimeMillis();
        System.out.println("Completed in " + (lastDumpTime - start) + "ms.");
    }

    public interface JacocoProxy {
        void dump(boolean reset);
    }
}
