/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

/**
 * Provides various implementations of <a
 * href="http://en.wikipedia.org/wiki/Cyclic_redundancy_check">cyclic redundancy
 * check</a> (CRC) error-detecting codes. CRC values are based on the remainder
 * of a polynomial division of the input data. They are particularly well-suited
 * to detecting burst errors in data sent over telecommunications channels or
 * retrieved from storage media.
 */
package com.scurrilous.circe.crc;
