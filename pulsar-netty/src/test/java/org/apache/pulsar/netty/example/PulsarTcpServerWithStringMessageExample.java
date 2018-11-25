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
package org.apache.pulsar.netty.example;

import io.netty.handler.codec.string.StringDecoder;
import org.apache.pulsar.netty.serde.PulsarSerializer;
import org.apache.pulsar.netty.tcp.server.PulsarTcpServer;

public class PulsarTcpServerWithStringMessageExample {

    private static String HOST = "localhost";
    private static int PORT = 8999;
    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-netty-topic";

    public static void main(String[] args) throws Exception {

        PulsarTcpServer<String> pulsarTcpServer = new PulsarTcpServer.Builder<String>()
                .setHost(HOST)
                .setPort(PORT)
                .setServiceUrl(SERVICE_URL)
                .setTopicName(TOPIC_NAME)
                .setNumberOfThreads(2)
                .setDecoder(new StringDecoder())
                .setPulsarSerializer(new MyUpperCaseStringSerializer())
                .build();

        pulsarTcpServer.run();
    }

    private static class MyUpperCaseStringSerializer implements PulsarSerializer<String> {

        @Override
        public byte[] serialize(String s) {
            return s.toUpperCase().getBytes();
        }

    }

}
