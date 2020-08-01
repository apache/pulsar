#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from __future__ import unicode_literals

from django.utils.encoding import python_2_unicode_compatible
from django.db.models import *
from django.urls import reverse

# Used to store the latest
class LatestTimestamp(Model):
    name = CharField(max_length=10, unique=True)
    timestamp = BigIntegerField(default=0)

@python_2_unicode_compatible
class Cluster(Model):
    name = CharField(max_length=200, unique=True)
    serviceUrl = URLField()

    def __str__(self):
        return self.name

@python_2_unicode_compatible
class Broker(Model):
    url = URLField(db_index=True)
    cluster = ForeignKey(Cluster, on_delete=SET_NULL, db_index=True, null=True)

    def __str__(self):
        return self.url

@python_2_unicode_compatible
class ActiveBroker(Model):
    broker    = ForeignKey(Broker, on_delete=SET_NULL, db_index=True, null=True)
    timestamp = BigIntegerField(db_index=True)

    def __str__(self):
        return self.broker.url

@python_2_unicode_compatible
class Property(Model):
    name = CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = 'properties'

@python_2_unicode_compatible
class Namespace(Model):
    name = CharField(max_length=200)
    property = ForeignKey(Property, on_delete=SET_NULL, db_index=True, null=True)
    clusters = ManyToManyField(Cluster)
    timestamp = BigIntegerField(db_index=True)
    deleted = BooleanField(default=False)

    def is_global(self):
        return self.name.split('/', 2)[1] == 'global'

    def is_v2(self):
        return len(self.name.split('/', 2)) == 2

    def __str__(self):
        return self.name

    class Meta:
        index_together = ('name', 'timestamp', 'deleted')

@python_2_unicode_compatible
class Bundle(Model):
    timestamp = BigIntegerField(db_index=True)
    broker = ForeignKey(Broker, on_delete=SET_NULL, db_index=True, null=True)
    namespace = ForeignKey(Namespace, on_delete=SET_NULL, db_index=True, null=True)
    cluster = ForeignKey(Cluster, on_delete=SET_NULL, db_index=True, null=True)
    range = CharField(max_length=200)

    def __str__(self):
        return str(self.pk) + '--' + self.namespace.name + '/' + self.range

@python_2_unicode_compatible
class Topic(Model):
    name      = CharField(max_length=1024, db_index=True)
    active_broker = ForeignKey(ActiveBroker, on_delete=SET_NULL, db_index=True, null=True)
    broker = ForeignKey(Broker, on_delete=SET_NULL, db_index=True, null=True)
    namespace = ForeignKey(Namespace, on_delete=SET_NULL, db_index=True, null=True)
    cluster = ForeignKey(Cluster, on_delete=SET_NULL, db_index=True, null=True)
    bundle = ForeignKey(Bundle, on_delete=SET_NULL, db_index=True, null=True)

    timestamp              = BigIntegerField(db_index=True)
    deleted                = BooleanField(default=False)
    averageMsgSize         = IntegerField(default=0)
    msgRateIn              = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgRateOut             = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgThroughputIn        = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgThroughputOut       = DecimalField(max_digits = 12, decimal_places=1, default=0)
    pendingAddEntriesCount = DecimalField(max_digits = 12, decimal_places=1, default=0)
    producerCount          = IntegerField(default=0)
    subscriptionCount      = IntegerField(default=0)
    consumerCount          = IntegerField(default=0)
    storageSize            = BigIntegerField(default=0)
    backlog                = BigIntegerField(default=0)

    localRateIn        = DecimalField(max_digits = 12, decimal_places=1, default=0)
    localRateOut       = DecimalField(max_digits = 12, decimal_places=1, default=0)
    localThroughputIn  = DecimalField(max_digits = 12, decimal_places=1, default=0)
    localThroughputOut = DecimalField(max_digits = 12, decimal_places=1, default=0)

    replicationRateIn        = DecimalField(max_digits = 12, decimal_places=1, default=0)
    replicationRateOut       = DecimalField(max_digits = 12, decimal_places=1, default=0)
    replicationThroughputIn  = DecimalField(max_digits = 12, decimal_places=1, default=0)
    replicationThroughputOut = DecimalField(max_digits = 12, decimal_places=1, default=0)
    replicationBacklog       = BigIntegerField(default=0)

    def short_name(self):
        return self.name.split('/', 5)[-1]

    def is_global(self):
        return self.namespace.is_global()

    def is_v2(self):
        return self.namespace.is_v2()

    def url_name(self):
        return '/'.join(self.name.split('://', 1))

    def get_absolute_url(self):
        url = reverse('topic', args=[self.url_name()])
        if self.namespace.is_global():
            url += '?cluster=' + self.cluster.name
        return url

    class Meta:
        index_together = ('name', 'cluster', 'timestamp', 'deleted')

    def __str__(self):
        return self.name

@python_2_unicode_compatible
class Subscription(Model):
    name             = CharField(max_length=200)
    topic            = ForeignKey(Topic, on_delete=SET_NULL, null=True)
    namespace        = ForeignKey(Namespace, on_delete=SET_NULL, null=True, db_index=True)

    timestamp        = BigIntegerField(db_index=True)
    deleted          = BooleanField(default=False)
    msgBacklog       = BigIntegerField(default=0)
    msgRateExpired   = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgRateOut       = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgRateRedeliver = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgThroughputOut = DecimalField(max_digits = 12, decimal_places=1, default=0)

    SUBSCRIPTION_TYPES = (
        ('N', 'Not connected'),
        ('E', 'Exclusive'),
        ('S', 'Shared'),
        ('F', 'Failover'),
    )
    subscriptionType = CharField(max_length=1, choices=SUBSCRIPTION_TYPES, default='N')
    unackedMessages  = BigIntegerField(default=0)

    class Meta:
        unique_together = ('name', 'topic', 'timestamp')

    def __str__(self):
        return self.name


class Consumer(Model):
    timestamp        = BigIntegerField(db_index=True)
    subscription     = ForeignKey(Subscription, on_delete=SET_NULL, db_index=True, null=True)

    address = CharField(max_length=64, null=True)
    availablePermits = IntegerField(default=0)
    connectedSince   = DateTimeField(null=True)
    consumerName     = CharField(max_length=256, null=True)
    msgRateOut       = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgRateRedeliver = DecimalField(max_digits = 12, decimal_places=1, default=0)
    msgThroughputOut = DecimalField(max_digits = 12, decimal_places=1, default=0)
    unackedMessages  = BigIntegerField(default=0)
    blockedConsumerOnUnackedMsgs = BooleanField(default=False)


class Replication(Model):
    timestamp      = BigIntegerField(db_index=True)
    topic          = ForeignKey(Topic, on_delete=SET_NULL, null=True)
    local_cluster  = ForeignKey(Cluster, on_delete=SET_NULL, null=True)
    remote_cluster = ForeignKey(Cluster, on_delete=SET_NULL, null=True, related_name='remote_cluster')

    msgRateIn               = DecimalField(max_digits = 12, decimal_places=1)
    msgThroughputIn         = DecimalField(max_digits = 12, decimal_places=1)
    msgRateOut              = DecimalField(max_digits = 12, decimal_places=1)
    msgThroughputOut        = DecimalField(max_digits = 12, decimal_places=1)
    msgRateExpired          = DecimalField(max_digits = 12, decimal_places=1)
    replicationBacklog      = BigIntegerField(default=0)

    connected  = BooleanField(default=False)
    replicationDelayInSeconds  = IntegerField(default=0)

    inboundConnectedSince   = DateTimeField(null=True)
    outboundConnectedSince   = DateTimeField(null=True)
