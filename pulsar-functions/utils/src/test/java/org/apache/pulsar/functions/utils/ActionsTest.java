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
package org.apache.pulsar.functions.utils;

import org.apache.pulsar.functions.utils.Actions;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ActionsTest {

    @Test
    public void testActionsSuccess() throws InterruptedException {

        // Test for success
        Supplier<Actions.ActionResult> supplier1 = mock(Supplier.class);
        when(supplier1.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

        Supplier<Actions.ActionResult> supplier2 = mock(Supplier.class);
        when(supplier2.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

        java.util.function.Consumer<Actions.ActionResult> onFail = mock(java.util.function.Consumer.class);
        java.util.function.Consumer<Actions.ActionResult> onSucess = mock(java.util.function.Consumer.class);

        Actions.Action action1 = spy(
            Actions.Action.builder()
                .actionName("action1")
                .numRetries(10)
                .sleepBetweenInvocationsMs(100)
                .supplier(supplier1)
                .continueOn(true)
                .onFail(onFail)
                .onSuccess(onSucess)
                .build());

        Actions.Action action2 = spy(
            Actions.Action.builder()
                .actionName("action2")
                .numRetries(20)
                .sleepBetweenInvocationsMs(200)
                .supplier(supplier2)
                .build());

        Actions actions = Actions.newBuilder()
            .addAction(action1)
            .addAction(action2);
        actions.run();

        assertEquals(actions.numActions(), 2);
        verify(supplier1, times(1)).get();
        verify(onFail, times(0)).accept(any());
        verify(onSucess, times(1)).accept(any());
        verify(supplier2, times(1)).get();
    }


    @Test
    public void testActionsOneAction() throws InterruptedException {
        // test only run 1 action
        Supplier<Actions.ActionResult> supplier1 = mock(Supplier.class);
        when(supplier1.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

        Supplier<Actions.ActionResult> supplier2 = mock(Supplier.class);
        when(supplier2.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

        java.util.function.Consumer<Actions.ActionResult> onFail = mock(java.util.function.Consumer.class);
        java.util.function.Consumer<Actions.ActionResult> onSucess = mock(java.util.function.Consumer.class);

        Actions.Action action1 = spy(
            Actions.Action.builder()
                .actionName("action1")
                .numRetries(10)
                .sleepBetweenInvocationsMs(100)
                .supplier(supplier1)
                .continueOn(false)
                .onFail(onFail)
                .onSuccess(onSucess)
                .build());
        Actions.Action action2 = spy(
            Actions.Action.builder()
                .actionName("action2")
                .numRetries(20)
                .sleepBetweenInvocationsMs(200)
                .supplier(supplier2)
                .onFail(onFail)
                .onSuccess(onSucess)
                .build());

        Actions actions = Actions.newBuilder()
            .addAction(action1)
            .addAction(action2);
        actions.run();

        assertEquals(actions.numActions(), 2);
        verify(supplier1, times(1)).get();
        verify(onFail, times(0)).accept(any());
        verify(onSucess, times(1)).accept(any());
        verify(supplier2, times(0)).get();
    }

    @Test
    public void testActionsRetry() throws InterruptedException {

      // test retry

        Supplier<Actions.ActionResult> supplier1 = mock(Supplier.class);
        when(supplier1.get()).thenReturn(Actions.ActionResult.builder().success(false).build());

        Supplier<Actions.ActionResult> supplier2 = mock(Supplier.class);
        when(supplier2.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

        java.util.function.Consumer<Actions.ActionResult> onFail = mock(java.util.function.Consumer.class);
        java.util.function.Consumer<Actions.ActionResult> onSucess = mock(java.util.function.Consumer.class);

        Actions.Action action1 = spy(
            Actions.Action.builder()
                .actionName("action1")
                .numRetries(10)
                .sleepBetweenInvocationsMs(10)
                .supplier(supplier1)
                .continueOn(false)
                .onFail(onFail)
                .onSuccess(onSucess)
                .build());

        Actions.Action action2 = spy(
            Actions.Action.builder()
                .actionName("action2")
                .numRetries(20)
                .sleepBetweenInvocationsMs(200)
                .supplier(supplier2)
                .build());

        Actions actions = Actions.newBuilder()
            .addAction(action1)
            .addAction(action2);
        actions.run();

        assertEquals(actions.numActions(), 2);
        verify(supplier1, times(11)).get();
        verify(onFail, times(1)).accept(any());
        verify(onSucess, times(0)).accept(any());
        verify(supplier2, times(1)).get();
    }

  @Test
  public void testActionsNoContinueOn() throws InterruptedException {
      // No continueOn
      Supplier<Actions.ActionResult>supplier1 = mock(Supplier.class);
      when(supplier1.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

      Supplier<Actions.ActionResult>    supplier2 = mock(Supplier.class);
      when(supplier2.get()).thenReturn(Actions.ActionResult.builder().success(true).build());

      java.util.function.Consumer<Actions.ActionResult> onFail = mock(java.util.function.Consumer.class);
      java.util.function.Consumer<Actions.ActionResult> onSucess = mock(java.util.function.Consumer.class);

      Actions.Action action1 = spy(
          Actions.Action.builder()
              .actionName("action1")
              .numRetries(10)
              .sleepBetweenInvocationsMs(100)
              .supplier(supplier1)
              .onFail(onFail)
              .onSuccess(onSucess)
              .build());

      Actions.Action action2 = spy(
          Actions.Action.builder()
              .actionName("action2")
              .numRetries(20)
              .sleepBetweenInvocationsMs(200)
              .supplier(supplier2)
              .build());

      Actions actions = Actions.newBuilder()
          .addAction(action1)
          .addAction(action2);
      actions.run();

      assertEquals(actions.numActions(), 2);
      verify(supplier1, times(1)).get();
      verify(onFail, times(0)).accept(any());
      verify(onSucess, times(1)).accept(any());
      verify(supplier2, times(1)).get();
    }
}