/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.discovery.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a dummy class will be overridden over {@link ZookeeperCacheLoader} in discovery module in order to avoid
 * ZooKeeper initialization
 *
 */
public class ZookeeperCacheLoader {

    public static final List<String> availableActiveBrokers = new ArrayList<String>();

    public ZookeeperCacheLoader(String zookeeperServers) throws InterruptedException, IOException {
        // dummy constructor
    }

    public List<String> getAvailableBrokers() {
        return this.availableActiveBrokers;
    }

    public void start() throws InterruptedException, IOException {
        // dummy method
    }
}
