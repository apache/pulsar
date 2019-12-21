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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Implements a streaming wordCount program on pulsar topics.
 *
 * <p>Example usage:
 *   pulsar://localhost:6650 test_src test_sub
 */
public class SparkStreamingPulsarReceiverExample {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Missing parameters!");
      System.err.println("Usage: <pulsar-service-url> <topic> <sub>");
      return;
    }

    String serviceUrl =  args[0];
    String inputTopic =  args[1];
    String subscription =  args[2];
    System.out.println("Parameters:");
    System.out.println("\tServiceUrl:\t" + serviceUrl);
    System.out.println("\tTopic:\t" + inputTopic);
    System.out.println("\tSubscription:\t" + subscription);

    SparkConf sparkConf = new SparkConf().setAppName("Pulsar Spark Example");

    JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(60));

    ConsumerConfigurationData<byte[]> pulsarConf = new ConsumerConfigurationData();

    Set<String> set = new HashSet<>();
    set.add(inputTopic);
    pulsarConf.setTopicNames(set);
    pulsarConf.setSubscriptionName(subscription);

    SparkStreamingPulsarReceiver pulsarReceiver = new SparkStreamingPulsarReceiver(
        serviceUrl,
        pulsarConf,
        new AuthenticationDisabled());

    JavaReceiverInputDStream<byte[]> lineDStream = jsc.receiverStream(pulsarReceiver);
    JavaPairDStream<String, Integer> result = lineDStream.flatMap(x -> {
        String line = new String(x, StandardCharsets.UTF_8);
        List<String> list = Arrays.asList(line.split(" "));
        return list.iterator();
      })
        .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
        .reduceByKey((x, y) -> x + y);

    result.print();

    jsc.start();
    jsc.awaitTermination();
  }

}
