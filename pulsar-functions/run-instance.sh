#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

java -cp runtime/target/java-instance.jar -Dlog4j.configurationFile=java_instance_log4j2.yml org.apache.pulsar.functions.runtime.instance.JavaInstanceMain \
    --name example \
    --tenant "test" \
    --namespace test-namespace \
    --source_topics persistent://sample/standalone/ns1/test_src \
    --sink_topic persistent://sample/standalone/ns1/test_result \
    --input_serde_classnames org.apache.pulsar.functions.api.utils.DefaultSerDe \
    --output_serde_classname org.apache.pulsar.functions.api.utils.DefaultSerDe \
    --processing_guarantees ATMOST_ONCE \
    --instance_id test-instance-id \
    --function_id test-function-id \
    --function_version "1.0" \
    --pulsar_serviceurl "pulsar://localhost:6650/" \
    --port 0 \
    --max_buffered_tuples 1000 \
    --function_classname org.apache.pulsar.functions.api.examples.CounterFunction \
    --state_storage_serviceurl localhost:4182 \
    --jar `pwd`/java-examples/target/pulsar-functions-api-examples.jar
