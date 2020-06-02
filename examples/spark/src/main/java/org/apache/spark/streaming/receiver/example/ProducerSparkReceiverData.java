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
package org.apache.spark.streaming.receiver.example;

import java.nio.charset.StandardCharsets;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * producer data to spark streaming receiver.
 *
 * <p>Example usage:
 *   pulsar://localhost:6650 test_src
 */
public class ProducerSparkReceiverData {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Missing parameters!");
      System.err.println("Usage: <pulsar-service-url> <topic>");
      return;
    }

    System.out.println("Parameters:");
    System.out.println("\tServiceUrl:\t" + args[0]);
    System.out.println("\tTopic:\t" + args[1]);

    try (PulsarClient client = PulsarClient.builder().serviceUrl(args[0]).build()) {
      try (Producer<byte[]> producer = client.newProducer().topic(args[1]).create()) {
        for (int i = 0; i < 100; i++) {
          producer.send(("producer spark streaming msg").getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    System.out.println("producer spark streaming msg end ...");
  }
}
