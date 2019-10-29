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

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;


/**
 * producer data to spark streaming receiver with Json/Pojo Object
 *
 * <p>Example usage:
 *   pulsar://localhost:6650 test_src
 */
public class ProducerSparkWithPojo {


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
			try (Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
					.topic(args[1]).sendTimeout(3, TimeUnit.SECONDS).create();) {
				for (int i = 0; i < 100; i++) {
					SensorReading rd = new SensorReading(i, "message " + i);
					producer.send(rd);
				}
			}
		}

		System.out.println("producer spark streaming msg end ...");
	}

}
