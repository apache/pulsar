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
///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.pulsar.functions.worker;
//
//import com.google.protobuf.InvalidProtocolBufferException;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.pulsar.client.admin.Namespaces;
//import org.apache.pulsar.client.admin.PulsarAdmin;
//import org.apache.pulsar.client.admin.Topics;
//import org.apache.pulsar.client.api.Consumer;
//import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
//import org.apache.pulsar.client.api.Message;
//import org.apache.pulsar.client.api.MessageId;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.client.api.PulsarClientException;
//import org.apache.pulsar.client.api.Reader;
//import org.apache.pulsar.client.api.Schema;
//import org.apache.pulsar.client.api.SubscriptionType;
//import org.apache.pulsar.functions.proto.Function;
//import org.apache.pulsar.functions.runtime.KubernetesRuntime;
//import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
//import org.testng.annotations.Test;
//
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//
//import static org.apache.pulsar.functions.utils.Utils.getFullyQualifiedInstanceId;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.doNothing;
//import static org.mockito.Mockito.doReturn;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.spy;
//
//@Slf4j
//public class TestTest {
//
//    @Test
//    public void test() throws Exception {
//
//        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://a3d2272b2ee1311e8b2db02f1a510fab-599210649.us-west-2.elb.amazonaws.com:6650").build();
//        Reader<byte[]> consumer = pulsarClient.newReader(Schema.BYTES)
//                .topic("system/functions-prod/assignments")
//                .cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
//                .startMessageId(MessageId.earliest)
//                .create();
//
//
//
//        WorkerConfig workerConfig = new WorkerConfig();
//        workerConfig.setWorkerId("c-prod-fw-prod-broker-2.prod-broker.default.svc.cluster.local-8080");
//        workerConfig.setPulsarServiceUrl("pulsar://a3d2272b2ee1311e8b2db02f1a510fab-599210649.us-west-2.elb.amazonaws.com:6650");
//        workerConfig.setStateStorageServiceUrl("foo");
//        workerConfig.setFunctionAssignmentTopicName("assignments");
//        workerConfig.setPulsarFunctionsNamespace("system/functions-prod");
//        workerConfig.setFunctionMetadataTopicName("metadata");
//        workerConfig.setKubernetesContainerFactory(new WorkerConfig.KubernetesContainerFactory().setSubmittingInsidePod(true));
//
//
//        FunctionMetaDataManager functionMetaDataManager = new FunctionMetaDataManager(workerConfig, mock(SchedulerManager.class), pulsarClient);
//
//        functionMetaDataManager.initialize();
//
//        log.info("all functions: {}", functionMetaDataManager.getAllFunctionMetaData());
//
//
//
//        WorkerService workerService = mock(WorkerService.class);
//        doReturn(pulsarClient).when(workerService).getClient();
//        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
//
//
//        KubernetesRuntimeFactory kubernetesRuntimeFactory = mock(KubernetesRuntimeFactory.class);
//        doReturn(true).when(kubernetesRuntimeFactory).externallyManaged();
//
//        doReturn(mock(KubernetesRuntime.class)).when(kubernetesRuntimeFactory).createContainer(any(), any(), any(), any());
//
//
//
//
//        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(workerConfig, workerService, null, null, null, functionMetaDataManager));
//
//
//        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
//        Namespaces namespaces = mock(Namespaces.class);
//        Topics topics = mock(Topics.class);
//
//        doNothing().when(namespaces).unsubscribeNamespace(any(), any());
//        doReturn(namespaces).when(pulsarAdmin).namespaces();
//        doReturn(topics).when(pulsarAdmin).topics();
//        doNothing().when(topics).deleteSubscription(any(), any());
//
//
//        FunctionActioner functionActioner = spy(new FunctionActioner(
//                workerConfig,
//                kubernetesRuntimeFactory, null, functionRuntimeManager.actionQueue, null, pulsarAdmin));
//
//        functionRuntimeManager.setFunctionActioner(functionActioner);
//
//
//
//
//
////        functionRuntimeManager.initialize();
//
////        log.info("functionRuntimeManager.workerIdToAssignments: {}", functionRuntimeManager.workerIdToAssignments);
////            log.info("functionRuntimeManager.functionRuntimeInfoMap: {}", functionRuntimeManager.functionRuntimeInfoMap);
//
//
//        FunctionAssignmentTailer tailer = new FunctionAssignmentTailer(functionRuntimeManager, consumer);
//
//
//
//
//
////        functionRuntimeManager.setInitializePhase(true);
////
////
////
////
//        while(consumer.hasMessageAvailable()) {
//            Message<byte[]> msg = consumer.readNext();
////            log.info("{} - {} - {} msg: {}", msg.getMessageId(), msg.getData().length, msg.getKey(), Function.Assignment.parseFrom(msg.getData()));
//
//            tailer.processAssignment(msg);
//
//            if (msg.getKey().equals("public/default/reverse3:-1")) {
//
//                log.info("functionRuntimeManager.workerIdToAssignments: {}", functionRuntimeManager.workerIdToAssignments);
//
//                log.info("functionRuntimeManager.functionRuntimeInfoMap: {}", functionRuntimeManager.functionRuntimeInfoMap);
//
//                log.info("functionRuntimeManager.actionQueue: {}", functionRuntimeManager.actionQueue.stream().filter(new Predicate<FunctionAction>() {
//
//
//                    @Override
//                    public boolean test(FunctionAction functionAction) {
//                        if (getFullyQualifiedInstanceId(functionAction.getFunctionRuntimeInfo().getFunctionInstance()).equals("public/default/reverse3:-1")){
//                            return true;
//                        }
//                        return false;
//                    }
//                }).collect(Collectors.toList()));
//            }
//
//        }
//
//
////        log.info("functionActioner.getActionQueue(): {}", functionActioner.getActionQueue());
//
//
//        functionRuntimeManager.actionQueue.forEach(new java.util.function.Consumer<FunctionAction>() {
//            @Override
//            public void accept(FunctionAction functionAction) {
//                functionActioner.processAction(functionAction);
//            }
//        });
//
//
//    }
//}
