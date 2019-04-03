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

import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.batch.connectors.pulsar.PulsarCsvOutputFormat
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled

/**
  * Implements a batch Scala program on Pulsar topic by writing Flink DataSet as Csv.
  */
object FlinkPulsarBatchCsvSinkScalaExample {

  /**
    * NasaMission Model
    */
  private case class NasaMission(id: Int, missionName: String, startYear: Int, endYear: Int)
    extends Tuple4(id, missionName, startYear, endYear)

  private val nasaMissions = List(
    NasaMission(1, "Mercury program", 1959, 1963),
    NasaMission(2, "Apollo program", 1961, 1972),
    NasaMission(3, "Gemini program", 1963, 1966),
    NasaMission(4, "Skylab", 1973, 1974),
    NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975))

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
    val pulsarCsvOutputFormat =
      new PulsarCsvOutputFormat[NasaMission](serviceUrl, topic, new AuthenticationDisabled())

    // create DataSet
    val textDS = env.fromCollection(nasaMissions)

    // map nasa mission names to upper-case
    textDS.map(nasaMission => NasaMission(
      nasaMission.id,
      nasaMission.missionName.toUpperCase,
      nasaMission.startYear,
      nasaMission.endYear))

    // filter missions which started after 1970
    .filter(_.startYear > 1970)

    // write batch data to Pulsar as Csv
    .output(pulsarCsvOutputFormat)

    // set parallelism to write Pulsar in parallel (optional)
    env.setParallelism(2)

    // execute program
    env.execute("Flink - Pulsar Batch Csv")
  }

}