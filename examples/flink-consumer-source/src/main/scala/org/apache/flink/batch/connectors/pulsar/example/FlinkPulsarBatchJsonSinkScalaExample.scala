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

import org.apache.flink.api.scala._
import org.apache.flink.batch.connectors.pulsar.PulsarJsonOutputFormat

import scala.beans.BeanProperty

/**
  * Implements a batch Scala program on Pulsar topic by writing Flink DataSet as Json.
  */
object FlinkPulsarBatchJsonSinkScalaExample {

  /**
    * NasaMission Model
    */
  private case class NasaMission(@BeanProperty id: Int,
                         @BeanProperty missionName: String,
                         @BeanProperty startYear: Int,
                         @BeanProperty endYear: Int)

  private val nasaMissions = List(
    NasaMission(1, "Mercury program", 1959, 1963),
    NasaMission(2, "Apollo program", 1961, 1972),
    NasaMission(3, "Gemini program", 1963, 1966),
    NasaMission(4, "Skylab", 1973, 1974),
    NasaMission(5, "Apollo–Soyuz Test Project", 1975, 1975))

  private val SERVICE_URL = "pulsar://127.0.0.1:6650"
  private val TOPIC_NAME = "my-flink-topic"

  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // create PulsarJsonOutputFormat instance
    val pulsarJsonOutputFormat = new PulsarJsonOutputFormat[NasaMission](SERVICE_URL, TOPIC_NAME)

    // create DataSet
    val nasaMissionDS = env.fromCollection(nasaMissions)

    // map nasa mission names to upper-case
    nasaMissionDS.map(nasaMission =>
      NasaMission(
        nasaMission.id,
        nasaMission.missionName.toUpperCase,
        nasaMission.startYear,
        nasaMission.endYear))

    // filter missions which started after 1970
    .filter(_.startYear > 1970)

    // write batch data to Pulsar
    .output(pulsarJsonOutputFormat)

    // set parallelism to write Pulsar in parallel (optional)
    env.setParallelism(2)

    // execute program
    env.execute("Flink - Pulsar Batch Json")
  }

}
