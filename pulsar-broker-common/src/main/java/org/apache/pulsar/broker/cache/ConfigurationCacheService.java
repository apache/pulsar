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
package org.apache.pulsar.broker.cache;

import java.util.Map;

import org.apache.bookkeeper.util.ZkUtils;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.impl.NamespaceIsolationPolicies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperChildrenCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;

/**
 * ConfigurationCacheService is only kept for compatibility as it was exposed in AuthorizationProvider interface.
 */
@Deprecated
public class ConfigurationCacheService {
}
