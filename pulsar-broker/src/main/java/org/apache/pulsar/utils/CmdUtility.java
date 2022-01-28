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
package org.apache.pulsar.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CmdUtility {
    private static final Logger LOG = LoggerFactory.getLogger(CmdUtility.class);
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    /**
     * Executes the specified string command in a separate process. STDOUT and STDERR output will be buffered to the
     * given <code>writer</code> ( {@link Writer}) argument.
     *
     * @param writer
     *            stdout and stderr output
     * @param command
     *            a specified system command
     * @return exitValue 0: success, Non-zero: failure
     * @throws IOException
     */
    public static int exec(Writer writer, String... command) throws IOException {
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (String str : command) {
                sb.append(str).append(' ');
            }
            LOG.debug("command={}", sb);
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process proc = null;
        BufferedReader reader = null;
        try {
            proc = pb.start();
            reader = new BufferedReader(new InputStreamReader(proc.getInputStream(), UTF_8));
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (writer != null) {
                    writer.write(line);
                    writer.write('\n');
                }
            }
            LOG.debug("sending the command to the host");
            int exitValue = proc.waitFor();
            if (LOG.isDebugEnabled()) {
                LOG.debug("command exit value={}", exitValue);
            }
            return exitValue;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (proc != null) {
                proc.destroy();
            }
        }
    }

}
