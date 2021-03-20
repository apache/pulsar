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
package org.apache.pulsar.functions.proto;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.ProcessingGuarantees;
import org.testng.annotations.Test;

/**
 * Unit test for {@link FunctionDetails}.
 */
public class FunctionDetailsTest {

    /**
     * Make sure the default processing guarantee is always `ATLEAST_ONCE`.
     */
    @Test
    public void testDefaultProcessingGuarantee() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(ProcessingGuarantees.ATLEAST_ONCE, fc.getProcessingGuarantees());
    }

    /**
     * Make sure the default value of batching on the function config is `UNKNOWN_BATCHING`.
     * (When that value is `UNKNOWN_BATCHING`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionBatchingOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.Batching.UNKNOWN_BATCHING, fc.getSink().getProducerSpec().getBatching());
    }
    /**
     * Make sure the default value of chunking on the function config is `UNKNOWN_CHUNKING`.
     * (When that value is `UNKNOWN_CHUNKING`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionChunkingOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.Chunking.UNKNOWN_CHUNKING, fc.getSink().getProducerSpec().getChunking());
    }
    /**
     * Make sure the default value of blocking on the function config is `UNKNOWN_BLOCKING`.
     * (When that value is `UNKNOWN_BLOCKING`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionBlockingOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.BlockIfQueueFull.UNKNOWN_BLOCKING, fc.getSink().getProducerSpec().getBlockIfQueueFull());
    }
    /**
     * Make sure the default value of compression on the function config is `UNKNOWN_COMPRESSION`.
     * (When that value is `UNKNOWN_COMPRESSION`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionCompressionOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.CompressionType.UNKNOWN_COMPRESSION, fc.getSink().getProducerSpec().getCompressionType());
    }

    /**
     * Make sure the default value of hashing on the function config is `UNKNOWN_HASHING`.
     * (When that value is `UNKNOWN_HASHING`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionHashingOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.HashingScheme.UNKNOWN_HASHING, fc.getSink().getProducerSpec().getHashingScheme());
    }
    /**
     * Make sure the default value of routing on the function config is `UNKNOWN_ROUTING`.
     * (When that value is `UNKNOWN_ROUTING`, the actual function's runtime value should inherit the default from the WorkerConfig.)
     */
    @Test
    public void testDefaultFunctionRoutingOnFunctionDetails() {
        FunctionDetails fc = FunctionDetails.newBuilder().build();
        assertEquals(Function.MessageRoutingMode.UNKNOWN_ROUTING, fc.getSink().getProducerSpec().getMessageRoutingMode());
    }
}
