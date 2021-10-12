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

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Slf4j
public class Actions {
    private List<Action> actions = new LinkedList<>();

    @Data
    @Builder(toBuilder=true)
    public static class Action {
        private String actionName;
        @Builder.Default
        private int numRetries = 1;
        private Supplier<ActionResult> supplier;
        @Builder.Default
        private long sleepBetweenInvocationsMs = 500;
        private Boolean continueOn;
        private Consumer<ActionResult> onFail;
        private Consumer<ActionResult> onSuccess;

        public void verifyAction() {
            if (isBlank(actionName)) {
                throw new RuntimeException("Action name is empty!");
            }
            if (supplier == null) {
                throw new RuntimeException("Supplier is not specified!");
            }
        }
    }

    @Data
    @Builder
    public static class ActionResult {
        private boolean success;
        private String errorMsg;
        private Object result;
    }

    private Actions() {

    }


    public Actions addAction(Action action) {
        action.verifyAction();
        this.actions.add(action);
        return this;
    }

    public static Actions newBuilder() {
        return new Actions();
    }

    public int numActions() {
        return actions.size();
    }

    public void run() throws InterruptedException {
        Iterator<Action> it = this.actions.iterator();
        while(it.hasNext()) {
            Action action  = it.next();

            boolean success;
            try {
                success = runAction(action);
            } catch (Exception e) {
                log.error("Uncaught exception thrown when running action [ {} ]:", action.getActionName(), e);
                success = false;
            }
            if (action.getContinueOn() != null) {
                if (success == action.getContinueOn()) {
                    continue;
                } else {
                    // terminate
                    break;
                }
            }
        }
    }

    private boolean runAction(Action action) throws InterruptedException {
        for (int i = 0; i< action.getNumRetries(); i++) {

            ActionResult actionResult = action.getSupplier().get();

            if (actionResult.isSuccess()) {
                log.info("Sucessfully completed action [ {} ]", action.getActionName());
                if (action.getOnSuccess() != null) {
                    action.getOnSuccess().accept(actionResult);
                }
                return true;
            } else {
                if (actionResult.getErrorMsg() != null) {
                    log.warn("Error completing action [ {} ] :- {} - [ATTEMPT] {}/{}",
                            action.getActionName(),
                            actionResult.getErrorMsg(),
                            i + 1, action.getNumRetries());
                } else {
                    log.warn("Error completing action [ {} ] [ATTEMPT] {}/{}",
                            action.getActionName(),
                            i + 1, action.getNumRetries());
                }

                Thread.sleep(action.sleepBetweenInvocationsMs);
            }
        }
        log.error("Failed completing action [ {} ]. Giving up!", action.getActionName());
        if (action.getOnFail() != null) {
            action.getOnFail().accept(action.getSupplier().get());
        }
        return false;
    }
}
