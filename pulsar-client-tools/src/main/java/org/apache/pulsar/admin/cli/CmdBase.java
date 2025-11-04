/*
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
package org.apache.pulsar.admin.cli;

import static org.apache.pulsar.client.admin.internal.BaseResource.getApiException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import picocli.CommandLine;

public abstract class CmdBase {
    private final CommandLine commander;
    private final Supplier<PulsarAdmin> adminSupplier;

    /**
     * Default request timeout in milliseconds.
     * Used if not found from configuration data in {@link #getRequestTimeoutMs()}
     */
    private static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 60000;

    public CmdBase(String cmdName, Supplier<PulsarAdmin> adminSupplier) {
        this.adminSupplier = adminSupplier;
        commander = new CommandLine(this);
        commander.setCommandName(cmdName);
    }

    public boolean run(String[] args) {
        return commander.execute(args) == 0;
    }

    protected PulsarAdmin getAdmin() {
        return adminSupplier.get();
    }

    protected long getRequestTimeoutMs() {
        PulsarAdmin pulsarAdmin = getAdmin();
        if (pulsarAdmin instanceof PulsarAdminImpl) {
            return ((PulsarAdminImpl) pulsarAdmin).getClientConfigData().getRequestTimeoutMs();
        }
        return DEFAULT_REQUEST_TIMEOUT_MILLIS;
    }

    protected <T> T sync(Supplier<CompletableFuture<T>> executor) throws PulsarAdminException {
        try {
            return executor.get().get(getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        } catch (ExecutionException e) {
            throw PulsarAdminException.wrap(getApiException(e.getCause()));
        } catch (Exception e) {
            throw PulsarAdminException.wrap(getApiException(e));
        }
    }

    Map<String, String> parseListKeyValueMap(List<String> metadata) {
        Map<String, String> map = null;
        if (metadata != null && !metadata.isEmpty()) {
            map = new HashMap<>();
            for (String property : metadata) {
                int pos = property.indexOf('=');
                if (pos <= 0) {
                    throw new ParameterException(String.format("Invalid key value pair '%s', "
                            + "valid format like 'a=b'.", property));
                }
                map.put(property.substring(0, pos), property.substring(pos + 1));
            }
        }
        return map;
    }

    // Used to register the subcomand.
    protected CommandLine getCommander() {
        return commander;
    }

    protected void addCommand(String name, Object cmd) {
        commander.addSubcommand(name, cmd);
    }

    protected void addCommand(String name, Object cmd, String... aliases) {
        commander.addSubcommand(name, cmd, aliases);
    }

    protected class ParameterException extends CommandLine.ParameterException {
        public ParameterException(String msg) {
            super(commander, msg);
        }

        public ParameterException(String msg, Throwable e) {
            super(commander, msg, e);
        }
    }
}
