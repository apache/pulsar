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
package org.apache.zookeeper.server.admin;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * CommandOutputters are used to format the responses from Commands.
 *
 * @see Command
 * @see JettyAdminServer
 */
public interface CommandOutputter {

    /**
     * The MIME type of this output (e.g., "application/json").
     */
    String getContentType();

    /**
     * Print out data as output.
     */
    default void output(CommandResponse response, PrintWriter pw) {
    }

    /**
     * Stream out data as output.
     */
    default void output(final CommandResponse response, final OutputStream os) {
    }
}
