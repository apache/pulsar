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
package org.apache.pulsar.broker.web;

import java.util.TimeZone;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;

/**
 * Class to standardize initialization of a Jetty request logger for all pulsar components.
 */
public class JettyRequestLogFactory {

    /**
     * The time format to use for request logging. This custom format is necessary because the
     * default option uses GMT for the time zone. Pulsar's request logging has historically
     * used the JVM's default time zone, so this format uses that time zone. It is also necessary
     * because the {@link CustomRequestLog#DEFAULT_DATE_FORMAT} is "dd/MMM/yyyy:HH:mm:ss ZZZ" instead
     * of "dd/MMM/yyyy:HH:mm:ss Z" (the old date format). The key difference is that ZZZ will render
     * the strict offset for the timezone that is unaware of daylight savings time while the Z will
     * render the offset based on daylight savings time.
     *
     * As the javadoc for {@link CustomRequestLog} describes, the time code can take two arguments to
     * configure the format and the time zone. They must be in the form: "%{format|timeZone}t".
     */
    private static final String TIME_FORMAT = String.format(" %%{%s|%s}t ",
            "dd/MMM/yyyy:HH:mm:ss Z",
            TimeZone.getDefault().getID());

    /**
     * This format is essentially the {@link CustomRequestLog#EXTENDED_NCSA_FORMAT} with three modifications:
     *   1. The time zone will be the JVM's default time zone instead of always being GMT.
     *   2. The time zone offset will be daylight savings time aware.
     *   3. The final value will be the request time (latency) in milliseconds.
     *
     * See javadoc for {@link CustomRequestLog} for more information.
     */
    private static final String LOG_FORMAT =
            "%{client}a - %u" + TIME_FORMAT + "\"%r\" %s %O \"%{Referer}i\" \"%{User-Agent}i\" %{ms}T";

    /**
     * Build a new Jetty request logger using the format defined in this class.
     * @return a request logger
     */
    public static CustomRequestLog createRequestLogger() {
        return new CustomRequestLog(new Slf4jRequestLogWriter(), LOG_FORMAT);
    }
}
