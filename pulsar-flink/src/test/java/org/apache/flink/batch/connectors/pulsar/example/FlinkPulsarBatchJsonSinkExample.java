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
package org.apache.flink.batch.connectors.pulsar.example;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.pulsar.PulsarJsonOutputFormat;

import java.util.Arrays;
import java.util.List;

/**
 * Implements a batch program on Pulsar topic by writing Flink DataSet as Json.
 */
public class FlinkPulsarBatchJsonSinkExample {

    private static final List<Employee> employees = Arrays.asList(
            new Employee(1, "John", "Tyson", "Engineering"),
            new Employee(2, "Pamela", "Moon", "HR"),
            new Employee(3, "Jim", "Sun", "Finance"),
            new Employee(4, "Michael", "Star", "Engineering"));

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create PulsarJsonOutputFormat instance
        final OutputFormat<Employee> pulsarJsonOutputFormat = new PulsarJsonOutputFormat<>(SERVICE_URL, TOPIC_NAME);

        // create DataSet
        DataSet<Employee> employeeDS = env.fromCollection(employees);
        // map employees' name, surname and department as upper-case
        employeeDS.map(employee -> new Employee(
                employee.id,
                employee.name.toUpperCase(),
                employee.surname.toUpperCase(),
                employee.department.toUpperCase()))
        // filter employees which is member of Engineering
        .filter(employee -> employee.department.equals("ENGINEERING"))
        // write batch data to Pulsar
        .output(pulsarJsonOutputFormat);

        env.setParallelism(2);

        // execute program
        env.execute("Flink - Pulsar Batch Json");
    }

    /**
     * Employee data model
     *
     * Note: Properties should be public or have getter function to be visible
     */
    private static class Employee {

        private long id;
        private String name;
        private String surname;
        private String department;

        public Employee(long id, String name, String surname, String department) {
            this.id = id;
            this.name = name;
            this.surname = surname;
            this.department = department;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getSurname() {
            return surname;
        }

        public String getDepartment() {
            return department;
        }
    }

}