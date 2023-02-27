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
package org.apache.pulsar.tests.integration.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.web.plugin.servlet.AdditionalServlet;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.eclipse.jetty.servlet.ServletHolder;

public class RandomAdditionalServlet extends HttpServlet implements AdditionalServlet {

    private int sequenceLength;

    @Override
    public void loadConfig(PulsarConfiguration pulsarConfiguration) {
        sequenceLength = Integer.parseInt(
                pulsarConfiguration.getProperties().getProperty("randomServletSequenceLength"));

    }

    @Override
    public String getBasePath() {
        return "/random";
    }

    @Override
    public ServletHolder getServletHolder() {
        return new ServletHolder(this);
    }

    @Override
    public void close() {

    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setContentType("text/plain");
        List<Integer> numbers = IntStream.range(0, sequenceLength).boxed()
                .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(numbers);
        String responseBody = numbers.stream().map(Object::toString).collect(Collectors.joining(","));
        ServletOutputStream output = resp.getOutputStream();
        output.write(responseBody.getBytes());
        output.close();
    }
}
