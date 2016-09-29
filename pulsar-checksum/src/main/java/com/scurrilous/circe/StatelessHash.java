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
package com.scurrilous.circe;

/**
 * Base interface for stateless hash functions that immediately return the hash
 * value corresponding to a given input. Stateless hash functions may be used
 * concurrently by multiple threads without any synchronization overhead.
 */
public interface StatelessHash extends Hash {

    /**
     * Returns a new instance of stateful version of this hash function.
     * 
     * @return the stateful version of this hash function
     */
    StatefulHash createStateful();
}
