<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Microbenchmarks for Apache Pulsar

This module contains microbenchmarks for Apache Pulsar.

## Running the benchmarks

The benchmarks are written using [JMH](http://openjdk.java.net/projects/code-tools/jmh/). To compile & run the benchmarks, use the following command:

```bash
# Compile everything for creating the shaded microbenchmarks.jar file
mvn -Pcore-modules,microbench,-main -T 1C clean package

# run the benchmarks using the standalone shaded jar in any environment
java -jar microbench/target/microbenchmarks.jar
```

For fast recompiling of the benchmarks (without compiling Pulsar modules) and creating the shaded jar, you can use the following command:

```bash
mvn -Pmicrobench -pl microbench clean package
```

