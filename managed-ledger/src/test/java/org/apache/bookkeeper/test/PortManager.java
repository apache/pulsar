/**
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
package org.apache.bookkeeper.test;

import java.net.ServerSocket;
import java.io.IOException;

/**
 * Port manager allows a base port to be specified on the commandline. Tests will then use ports, counting up from this
 * base port. This allows multiple instances of the bookkeeper tests to run at once.
 */
public class PortManager {
    private static int nextPort = getBasePort();

    public synchronized static int nextFreePort() {
        while (true) {
            ServerSocket ss = null;
            try {
                int port = nextPort++;
                ss = new ServerSocket(port);
                ss.setReuseAddress(true);
                return port;
            } catch (IOException ioe) {
            } finally {
                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException ioe) {
                    }
                }
            }
        }
    }

    private static int getBasePort() {
        return Integer.valueOf(System.getProperty("test.basePort", "15000"));
    }
}
