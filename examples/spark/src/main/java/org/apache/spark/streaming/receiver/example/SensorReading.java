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

/**
 * Rest Pojo class to send to Producer
 *
 */
public class SensorReading {

	public SensorReading(int sensor_id, String sensor_value) {
		super();
		this.sensor_id = sensor_id;
		this.sensor_value = sensor_value;
	}

	public int getSensor_id() {
		return sensor_id;
	}

	public void setSensor_id(int sensor_id) {
		this.sensor_id = sensor_id;
	}

	public String getSensor_value() {
		return sensor_value;
	}

	public void setSensor_value(String sensor_value) {
		this.sensor_value = sensor_value;
	}

	public int sensor_id;

	public String sensor_value;

}
