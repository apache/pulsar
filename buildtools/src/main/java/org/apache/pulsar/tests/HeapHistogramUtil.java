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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.management.JMException;
import javax.management.ObjectName;

public class HeapHistogramUtil {
    public static String buildHeapHistogram() {
        StringBuilder dump = new StringBuilder();
        dump.append(String.format("Timestamp: %s", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now())));
        dump.append("\n\n");
        try {
            dump.append(callDiagnosticCommand("gcHeapInfo"));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        dump.append("\n");
        try {
            dump.append(callDiagnosticCommand("gcClassHistogram"));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return dump.toString();
    }

    /**
     * Calls a diagnostic commands.
     * The available operations are similar to what the jcmd commandline tool has,
     * however the naming of the operations are different. The "help" operation can be used
     * to find out the available operations. For example, the jcmd command "Thread.print" maps
     * to "threadPrint" operation name.
     */
    static String callDiagnosticCommand(String operationName, String... args)
            throws JMException {
        return (String) ManagementFactory.getPlatformMBeanServer()
                .invoke(new ObjectName("com.sun.management:type=DiagnosticCommand"),
                        operationName, new Object[] {args}, new String[] {String[].class.getName()});
    }
}
