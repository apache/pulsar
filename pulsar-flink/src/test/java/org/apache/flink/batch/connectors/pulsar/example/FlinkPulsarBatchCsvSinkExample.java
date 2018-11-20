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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.pulsar.PulsarCsvOutputFormat;

import java.util.Arrays;
import java.util.List;

/**
 * Implements a batch program on Pulsar topic by writing Flink DataSet as Csv.
 */
public class FlinkPulsarBatchCsvSinkExample {

    private static final List<Employee> employees = Arrays.asList(
            new Employee(1, "John", "Tyson", "Engineering", "Test"),
            new Employee(2, "Pamela", "Tyson", "HR", "Test"),
            new Employee(3, "Jim", "Sun", "Finance", "Test"),
            new Employee(4, "Michael", "Star", "Engineering", "Test"));

    private static final List<String> fieldNames = Arrays.asList("id", "name", "surname", "department");

    private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
    private static final String TOPIC_NAME = "my-flink-topic";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create PulsarOutputFormat instance
        final OutputFormat<Employee> pulsarCsvOutputFormat =
                new PulsarCsvOutputFormat<>(SERVICE_URL, TOPIC_NAME, fieldNames);

        // create DataSet
        DataSet<Employee> textDS = env.fromCollection(employees);

        textDS.map(new MapFunction<Employee, Employee>() {
            @Override
            public Employee map(Employee employee) throws Exception {
                return new Employee(employee.id,
                        employee.name.toUpperCase(),
                        employee.surname.toUpperCase(),
                        employee.department.toUpperCase(),
                        employee.company.toUpperCase());
            }
        })
        // filter employees which is member of Engineering
        .filter(wordWithCount -> wordWithCount.department.equals("Engineering"))
        // write batch data to Pulsar
        .output(pulsarCsvOutputFormat);

        // execute program
        env.execute("Flink - Pulsar Batch Csv");

    }

    /**
     * Data type for Employee Model.
     */
    private static class Employee {

        public long id;
        public String name;
        public String surname;
        public String department;
        public String company;

        public Employee(long id, String name, String surname, String department, String company) {
            this.id = id;
            this.name = name;
            this.surname = surname;
            this.department = department;
            this.company = company;
        }

    }
}