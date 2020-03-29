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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.avro.generated.NasaMission
import org.apache.flink.batch.connectors.pulsar.PulsarAvroOutputFormat
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled

/**
  * Implements a batch Scala program on Pulsar topic by writing Flink DataSet as Avro.
  */
object FlinkPulsarBatchAvroSinkScalaExample {

  private val nasaMissions = List(
    NasaMission.newBuilder.setId(1).setName("Mercury program").setStartYear(1959).setEndYear(1963).build,
    NasaMission.newBuilder.setId(2).setName("Apollo program").setStartYear(1961).setEndYear(1972).build,
    NasaMission.newBuilder.setId(3).setName("Gemini program").setStartYear(1963).setEndYear(1966).build,
    NasaMission.newBuilder.setId(4).setName("Skylab").setStartYear(1973).setEndYear(1974).build,
    NasaMission.newBuilder.setId(5).setName("Apollo–Soyuz Test Project").setStartYear(1975).setEndYear(1975).build)

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

    // create PulsarCsvOutputFormat instance
    val pulsarAvroOutputFormat =
      new PulsarAvroOutputFormat[NasaMission](serviceUrl, topic, new AuthenticationDisabled())

    // create DataSet
    val textDS = env.fromCollection(nasaMissions)

    // map nasa mission names to upper-case
    textDS.map(nasaMission => new NasaMission(
      nasaMission.getId,
      nasaMission.getName,
      nasaMission.getStartYear,
      nasaMission.getEndYear))

      // filter missions which started after 1970
      .filter(_.getStartYear > 1970)

      // write batch data to Pulsar as Avro
      .output(pulsarAvroOutputFormat)

    // set parallelism to write Pulsar in parallel (optional)
    env.setParallelism(2)

    // execute program
    env.execute("Flink - Pulsar Batch Avro")
  }

}
