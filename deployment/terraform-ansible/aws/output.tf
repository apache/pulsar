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

output "dns_name" {
  value = "${aws_elb.default.dns_name}"
}

output "pulsar_service_url" {
  value = "pulsar://${aws_elb.default.dns_name}:6650"
}

output "pulsar_web_url" {
  value = "http://${aws_elb.default.dns_name}:8080"
}

output "pulsar_ssh_host" {
  value = "${aws_instance.proxy.0.public_ip}"
}
