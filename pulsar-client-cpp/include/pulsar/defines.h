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
#ifndef PULSAR_DEFINES_H_
#define PULSAR_DEFINES_H_

#ifdef PULSAR_STATIC

#define PULSAR_PUBLIC

#else

#ifdef _WIN32

#ifdef BUILDING_PULSAR
#define PULSAR_PUBLIC __declspec(dllexport)
#else
#define PULSAR_PUBLIC __declspec(dllimport)
#endif /*BUILDING_PULSAR*/

#else

#define PULSAR_PUBLIC __attribute__((visibility("default")))

#endif /*_WIN32*/

#endif /*PULSAR_STATIC*/

#endif /* PULSAR_DEFINES_H_ */
