/*
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

// Add Maven command line arguments
def mavenCommand = ''
if (System.env.MAVEN_CMD_LINE_ARGS) {
    mavenCommand = "mvn ${System.env.MAVEN_CMD_LINE_ARGS}".toString()
    buildScan.value('Maven command line', mavenCommand)
}
if (System.env.GITHUB_ACTIONS) {
    if (session.goals.size() == 1 && session.goals[0] == 'initialize') {
        // omit publishing build scan when the only goal is "initialize"
        buildScan.publishOnDemand()
    } else {
        def jobName = System.env.JOB_NAME ?: System.env.GITHUB_JOB
        buildScan.value('GitHub Actions Job name', jobName)
        buildScan.value('GitHub Actions Event name', System.env.GITHUB_EVENT_NAME)
        buildScan.value('GitHub Ref name', System.env.GITHUB_REF_NAME)
        buildScan.value('GitHub Actor', System.env.GITHUB_ACTOR)
        buildScan.link('GitHub Repository', "https://github.com/" + System.env.GITHUB_REPOSITORY)
        buildScan.link('GitHub Commit', "https://github.com/" + System.env.GITHUB_REPOSITORY + "/commits/" + System.env.GITHUB_SHA)
        buildScan.buildScanPublished {  publishedBuildScan ->
            new File(System.env.GITHUB_STEP_SUMMARY).withWriterAppend { out ->
                out.println("\n[Gradle build scan for '${mavenCommand}' in ${jobName}](${publishedBuildScan.buildScanUri})\n")
            }
        }
    }
}