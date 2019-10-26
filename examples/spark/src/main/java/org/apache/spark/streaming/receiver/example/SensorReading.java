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
