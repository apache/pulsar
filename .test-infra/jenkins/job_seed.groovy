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
import common_job_properties

// Defines the seed job, which creates or updates all other Jenkins projects.
job('pulsar-seed') {
  description('Automatically configures all Apache Pulsar Jenkins projects based' +
              ' on Jenkins DSL groovy files checked into the code repository.')

  // Set common parameters.
  common_job_properties.setTopLevelMainJobProperties(delegate)

  // This is a post-commit job that runs once per day, not for every push.
  common_job_properties.setPostCommit(
      delegate,
      'H 6 * * *',
      false,
      'dev@pulsar.apache.org')

  // Allows triggering this build against pull requests.
  common_job_properties.enablePhraseTriggeringFromPullRequest(
    delegate,
    'Seed Job',
    '/seed')

  steps {
    dsl {
      // A list or a glob of other groovy files to process.
      external('.test-infra/jenkins/job_*.groovy')

      // If a job is removed from the script, delete it
      removeAction('DELETE')
    }
  }
}
