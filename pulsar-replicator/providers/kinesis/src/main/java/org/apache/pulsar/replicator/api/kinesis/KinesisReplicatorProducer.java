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
package org.apache.pulsar.replicator.api.kinesis;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.replicator.api.ReplicatorProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

/**
 * Kinesis producer that reads pulsar message and publishes to configured
 * kinesis stream.
 *
 */
public class KinesisReplicatorProducer implements ReplicatorProducer {

	// TODO: document it
	public static final String KINESIS_PARTITIONED_KEY = "kinesis.partitioned.key";
	public static final String KINESIS_LOG_LEVEL = "kinesis.partitioned.key";
	public static final String KINESIS_METRICS_LEVEL = "kinesis.partitioned.key";
	private String topicName;
	private String streamName;
	private KinesisProducer kinesisProducer;

	public KinesisReplicatorProducer(String topicName, String streamName, Region region, AWSCredentials credentials, Map<String, String> replicatorProperties) {
		this.topicName = topicName;
		this.streamName = streamName;
		
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion(region.getName());
		config.setMetricsLevel(getOrDefault(replicatorProperties,KINESIS_METRICS_LEVEL, "none"));
		config.setLogLevel(getOrDefault(replicatorProperties,KINESIS_LOG_LEVEL, "info"));
		config.setFailIfThrottled(true);
		// to maintain message-ordering: we are sending message sequentially which
		// requires aggregation disabled
		config.setAggregationEnabled(false);
		AWSCredentialsProvider credentialProvider = new AWSCredentialsProvider() {
			@Override
			public AWSCredentials getCredentials() {
				return credentials;
			}
			@Override
			public void refresh() {
				// No-op
			}
		};
		config.setCredentialsProvider(credentialProvider);
		this.kinesisProducer = new KinesisProducer(config);
	}

    @Override
	public CompletableFuture<Void> send(Message message) {
		if (log.isDebugEnabled()) {
			log.debug("[{}] Sending message to stream {} with size {}", this.topicName, this.streamName,
					message.getData().length);
		}
		CompletableFuture<Void> future = new CompletableFuture<>();
		final String partitionedKey = message.hasProperty(KINESIS_PARTITIONED_KEY)
				? message.getProperty(KINESIS_PARTITIONED_KEY)
				: Long.toString(System.currentTimeMillis());
		ListenableFuture<UserRecordResult> addRecordResult = kinesisProducer.addUserRecord(this.streamName,
				partitionedKey, ByteBuffer.wrap(message.getData()));
		addCallback(addRecordResult, ProducerSendCallback.create(this.streamName, future, System.nanoTime()),
				directExecutor());
		return future;
	}

	@Override
	public void close() {
		if (kinesisProducer != null) {
			kinesisProducer.flush();
			kinesisProducer.destroy();
		}
	}

	private static final class ProducerSendCallback implements FutureCallback<UserRecordResult> {

		private CompletableFuture<Void> result;
		private String streamName;
		private final Handle<ProducerSendCallback> recyclerHandle;
		private long startTime = 0;

		private ProducerSendCallback(Handle<ProducerSendCallback> recyclerHandle) {
			this.recyclerHandle = recyclerHandle;
		}

		static ProducerSendCallback create(String streamName, CompletableFuture<Void> result, long startTime) {
			ProducerSendCallback sendCallback = RECYCLER.get();
			sendCallback.result = result;
			sendCallback.streamName = streamName;
			sendCallback.startTime = startTime;
			return sendCallback;
		}

		private void recycle() {
			result = null;
			streamName = null;
			recyclerHandle.recycle(this);
		}

		private static final Recycler<ProducerSendCallback> RECYCLER = new Recycler<ProducerSendCallback>() {
			@Override
			protected ProducerSendCallback newObject(Handle<ProducerSendCallback> handle) {
				return new ProducerSendCallback(handle);
			}
		};

		@Override
		public void onSuccess(UserRecordResult result) {
			if (log.isDebugEnabled()) {
				log.debug("Successfully published message for replicator of {}-{} with latency", this.streamName,
						result.getShardId(), TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - startTime)));
			}
			this.result.complete(null);
			recycle();
		}

		@Override
		public void onFailure(Throwable exception) {
			this.result.completeExceptionally(exception);
			log.error("Failed to published message for replicator of {} ", this.streamName);
			recycle();
		}
	}

	private String getOrDefault(Map<String, String> properties, String key, String defaultVal) {
        return properties!=null && properties.containsKey(key) ? properties.get(key) : defaultVal;
    }
	
	private static final Logger log = LoggerFactory.getLogger(KinesisReplicatorProducer.class);
}
