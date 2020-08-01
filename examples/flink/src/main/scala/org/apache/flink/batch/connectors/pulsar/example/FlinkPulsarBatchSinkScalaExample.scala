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
package org.apache.flink.batch.connectors.pulsar.example

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.batch.connectors.pulsar.PulsarOutputFormat
import org.apache.flink.util.Collector
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled

/**
  * Data type for words with count.
  */
case class WordWithCount(word: String, count: Long) {
  override def toString: String = "WordWithCount { word = " + word + ", count = " + count + " }"
}

/**
  * Implements a batch word-count Scala program on Pulsar topic by writing Flink DataSet.
  */
object FlinkPulsarBatchSinkScalaExample {

  private val EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
    "Knowledge is limited. Imagination encircles the world."

  def main(args: Array[String]): Unit = {

    // parse input arguments
    val parameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 2) {
      println("Missing parameters!")
      println("Usage: pulsar --service-url <pulsar-service-url> --topic <topic>")
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(parameterTool)

    val serviceUrl = parameterTool.getRequired("service-url")
    val topic = parameterTool.getRequired("topic")

    println("Parameters:")
    println("\tServiceUrl:\t" + serviceUrl)
    println("\tTopic:\t" + topic)

    // create PulsarOutputFormat instance
    val pulsarOutputFormat =
      new PulsarOutputFormat[WordWithCount](serviceUrl, topic, new AuthenticationDisabled(), new SerializationSchema[WordWithCount] {
        override def serialize(wordWithCount: WordWithCount): Array[Byte] = wordWithCount.toString.getBytes
      })

    // create DataSet
    val textDS = env.fromElements[String](EINSTEIN_QUOTE)

    // convert sentence to words
    textDS.flatMap((value: String, out: Collector[WordWithCount]) => {
      val words = value.toLowerCase.split(" ")
      for (word <- words) {
        out.collect(new WordWithCount(word.replace(".", ""), 1))
      }
    })

    // filter words which length is bigger than 4
    .filter((wordWithCount: WordWithCount) => wordWithCount.word.length > 4)

    // group the words
    .groupBy((wordWithCount: WordWithCount) => wordWithCount.word)

    // sum the word counts
    .reduce((wordWithCount1: WordWithCount, wordWithCount2: WordWithCount) =>
      new WordWithCount(wordWithCount1.word, wordWithCount1.count + wordWithCount2.count))

    // write batch data to Pulsar
    .output(pulsarOutputFormat)

    // set parallelism to write Pulsar in parallel (optional)
    env.setParallelism(2)

    // execute program
    env.execute("Flink - Pulsar Batch WordCount")
  }

}