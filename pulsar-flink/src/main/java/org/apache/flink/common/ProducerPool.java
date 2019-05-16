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

package org.apache.flink.common;

import org.apache.pulsar.client.api.Producer;

import java.util.HashMap;
import java.util.Map;

/**
 * This is used for share Pulsar Producer across Flink Tasks.
 * @param <T> type of {@link Producer}
 */
public class ProducerPool<T> {

	/**
	 * producer name to Producer.
	 */
	private final Map<String, Producer<T>> producers = new HashMap<>();

	/**
	 * keep record of reference count of each Producer.
	 */
	private final Map<String, Integer> refCntMap = new HashMap<>();

	public synchronized boolean contains(String producerName) {
		return producers.get(producerName) != null;
	}

	public synchronized Producer<T> getProducer(String name) {
		if (!contains(name)) {
			throw new RuntimeException("Can't find a producer with name: " + name);
		}

		Integer refCnt = refCntMap.get(name);
		refCntMap.put(name, refCnt + 1);
		return producers.get(name);
	}

	public synchronized void addProducer(String name, Producer<T> producer) {
		if (contains(name)) {
			throw new RuntimeException("Producer already exists with name: " + name);
		}

		refCntMap.put(name, 1);
		producers.put(name, producer);
	}

	/**
	 *
	 * @param name the name of producer
	 * @return return true indicates the producer is no longer occupied by other threads
	 */
	public synchronized boolean removeProducer(String name) {
		Integer refCnt = refCntMap.get(name);
		if (refCnt == null) {
			return false;
		}

		if (refCnt == 1) {
			producers.remove(name);
			refCntMap.remove(name);
			return true;
		} else {
			refCntMap.put(name, refCnt - 1);
			return false;
		}
	}
}
