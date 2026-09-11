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
package org.apache.zookeeper.server.admin;

import java.lang.reflect.InvocationTargetException;
import lombok.CustomLog;

/**
 * Factory class for creating an AdminServer.
 */
@CustomLog
public class AdminServerFactory {
    /**
     * This method encapsulates the logic for whether we should use a
     * JettyAdminServer (i.e., the AdminServer is enabled) or a DummyAdminServer
     * (i.e., the AdminServer is disabled). It uses reflection when attempting
     * to create a JettyAdminServer, rather than referencing the class directly,
     * so that it's ok to omit Jetty from the classpath if a user doesn't wish
     * to pull in Jetty with ZooKeeper.
     */
    public static AdminServer createAdminServer() {
        if (!"false".equals(System.getProperty("zookeeper.admin.enableServer"))) {
            try {
                Class<?> jettyAdminServerC = Class.forName("org.apache.zookeeper.server.admin.JettyAdminServer");
                Object adminServer = jettyAdminServerC.getConstructor().newInstance();
                return (AdminServer) adminServer;

            } catch (ClassNotFoundException e) {
                log.warn().exception(e).log("Unable to start JettyAdminServer");
            } catch (InstantiationException e) {
                log.warn().exception(e).log("Unable to start JettyAdminServer");
            } catch (IllegalAccessException e) {
                log.warn().exception(e).log("Unable to start JettyAdminServer");
            } catch (InvocationTargetException e) {
                log.warn().exception(e).log("Unable to start JettyAdminServer");
            } catch (NoSuchMethodException e) {
                log.warn().exception(e).log("Unable to start JettyAdminServer");
            } catch (NoClassDefFoundError e) {
                log.warn().exception(e).log("Unable to load jetty, not starting JettyAdminServer");
            }
        }
        return new DummyAdminServer();
    }

}
