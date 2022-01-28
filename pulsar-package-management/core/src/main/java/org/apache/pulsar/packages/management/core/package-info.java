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

/**
 * Packages management provides an easier way to manage the packages.
 * We abstract the storage so we can store the packages anywhere. And we can store the
 * packages on the cloud to support an registry for all the pulsar package such as functions,
 * connectors, and other things.
 * Also, packages management have a version control that will make the function upgrade and
 * rollback more easier to track.
 */
package org.apache.pulsar.packages.management.core;
