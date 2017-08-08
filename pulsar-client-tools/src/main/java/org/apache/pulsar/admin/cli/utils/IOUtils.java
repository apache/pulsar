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
/**
 * This file is derived from IOUtils from Apache BookKeeper
 * http://bookkeeper.apache.org
 */

package org.apache.pulsar.admin.cli.utils;

import java.io.IOException;

public class IOUtils {

    /**
     * Confirm prompt for the console operations.
     *
     * @param prompt
     *            Prompt message to be displayed on console
     * @return Returns true if confirmed as 'Y', returns false if confirmed as 'N'
     * @throws IOException
     */
    public static boolean confirmPrompt(String prompt) throws IOException {
        while (true) {
            System.out.print(prompt + " (Y or N) ");
            StringBuilder responseBuilder = new StringBuilder();
            while (true) {
                int c = System.in.read();
                if (c == -1 || c == '\r' || c == '\n') {
                    break;
                }
                responseBuilder.append((char) c);
            }

            String response = responseBuilder.toString();
            if (response.equalsIgnoreCase("y") || response.equalsIgnoreCase("yes")) {
                return true;
            } else if (response.equalsIgnoreCase("n") || response.equalsIgnoreCase("no")) {
                return false;
            }
            System.out.println("Invalid input: " + response);
            // else ask them again
        }
    }
}
