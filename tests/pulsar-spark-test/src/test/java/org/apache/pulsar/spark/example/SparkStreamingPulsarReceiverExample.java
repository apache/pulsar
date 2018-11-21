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
package org.apache.pulsar.spark.example;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamingPulsarReceiverExample {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pulsar-spark");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();
        String url = "pulsar://localhost:6650/";
        String topic = "persistent://sample/standalone/ns1/topic1";
        String subs = "sub1";

        JavaReceiverInputDStream<byte[]> msgs = jssc
                .receiverStream(new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));

        JavaDStream<Integer> isContainingPulsar = msgs.flatMap(new FlatMapFunction<byte[], Integer>() {
            @Override
            public Iterator<Integer> call(byte[] msg) {
                return Arrays.asList(((new String(msg)).indexOf("Pulsar") != -1) ? 1 : 0).iterator();
            }
        });

        JavaDStream<Integer> numOfPulsar = isContainingPulsar.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        numOfPulsar.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
