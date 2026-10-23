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

# Client concurrency tests

Run the optional [jcstress](https://openjdk.org/projects/code-tools/jcstress/) suite:

```bash
./gradlew :pulsar-client-original:jcstress
```

Override the runner options for a short smoke run or a longer investigation:

```bash
./gradlew :pulsar-client-original:jcstress --args='-t org.apache.pulsar.client.impl.MultiTopics.*Stress -m sanity -f 1 -fsm 1 -iters 1 -time 100 -c 4 -sc false -af NONE -strideSize 1 -strideCount 1 -r ../reports/jcstress'
```

The default run limits CPU, heap and state batches because each state constructs real consumers.
Reports are written to `pulsar-client/build/reports/jcstress`; binary results are retained in
`pulsar-client/build/jcstress`. The source set and runner are separate from ordinary unit tests and
published client dependencies. Check source style with `:pulsar-client-original:checkstyleJcstress`.

The four cases race non-partitioned or partitioned child publication against public parent
`pause()` or `resume()`. Their arbiters require every published child to match the final parent
state and close all consumers. Schema preprocessing completes immediately; connection lookup stays
pending. A direct executor runs cleanup after both actors finish. No actor synchronization,
reflection, broker connection, timer or background executor thread is involved.

These cases exercise the production creation and publication paths. Ordinary regression tests
cover the controlled interleaving, initial subscription and reconnection FLOW behavior. A finite
jcstress run without forbidden outcomes provides supporting evidence, not a proof of correctness.
