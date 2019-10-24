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

import logging
import struct
import chardet
import hexdump
import requests
import json
import re

from django.shortcuts import render, get_object_or_404, redirect
from django.views import generic
from dashboard import settings

from stats.models import *
from stats.templatetags.table import Table
from utils import import_utils

pulsar = import_utils.try_import("pulsar")
logger = logging.getLogger(__name__)


class AdminPath:
    v1 = "/admin"
    v2 = "/admin/v2"

    @staticmethod
    def get(is_v2):
        return AdminPath.v2 if is_v2 else AdminPath.v1


class TopicName:
    def __init__(self, topic_name):
        try:
            self.scheme, self.path = topic_name.split("://", 1)
        except ValueError:
            self.scheme, self.path = topic_name.split("/", 1)
        self.namespace_path, self.name = self.path.rsplit("/", 1)
        self.namespace = NamespaceName(self.namespace_path)

    def is_v2(self):
        return self.namespace.is_v2()

    def url_name(self):
        return "/".join([self.scheme, self.path])

    def full_name(self):
        return "://".join([self.scheme, self.path])

    def short_name(self):
        return self.name

    def is_global(self):
        return self.namespace.is_global()

    def admin_path(self):
        b = AdminPath.get(self.is_v2())
        return settings.SERVICE_URL + b + "/" + self.url_name()


class NamespaceName:

    def __init__(self, namespace_name):
        self.path = namespace_name.strip("/")
        path_parts = self.path.split("/")
        if len(path_parts) == 2:
            self.tenant, self.namespace = path_parts
            self.cluster = None
        elif len(path_parts) == 3:
            self.tenant, self.cluster, self.namespace = path_parts
        else:
            raise ValueError("invalid namespace:" + namespace_name)

    def is_v2(self):
        return self.cluster is None

    def is_global(self):
        return self.cluster == "global"

    def admin_path(self):
        b = AdminPath.get(self.is_v2())
        return settings.SERVICE_URL + b + "/namespaces/" + self.path


def get_timestamp():
    try:
        return LatestTimestamp.objects.get(name='latest').timestamp
    except:
        return 0

class HomeView(generic.ListView):
    model = Property
    template_name = 'stats/home.html'

def home(request):
    ts = get_timestamp()
    properties = Property.objects.filter(
        namespace__topic__timestamp = ts,
    ).annotate(
        numNamespaces = Count('namespace__name', distinct=True),
        numTopics    = Count('namespace__topic__name', distinct=True),
        numProducers = Sum('namespace__topic__producerCount', filter=Q(topic__timestamp__eq=ts)),
        numSubscriptions = Sum('namespace__topic__subscriptionCount'),
        numConsumers  = Sum('namespace__topic__consumerCount'),
        backlog       = Sum('namespace__topic__backlog'),
        storage       = Sum('namespace__topic__storageSize'),
        rateIn        = Sum('namespace__topic__msgRateIn'),
        rateOut       = Sum('namespace__topic__msgRateOut'),
        throughputIn  = Sum('namespace__topic__msgThroughputIn'),
        throughputOut = Sum('namespace__topic__msgThroughputOut'),
    )

    logger.info(properties.query)

    properties = Table(request, properties, default_sort='name')

    return render(request, 'stats/home.html', {
        'properties': properties,
        'title' : 'Tenants',
    })

def property(request, property_name):
    property = get_object_or_404(Property, name=property_name)
    ts = get_timestamp()
    namespaces = Namespace.objects.filter(
            property = property,
            timestamp = ts,
            deleted = False,
        ).annotate(
            numTopics    = Count('topic'),
            numProducers = Sum('topic__producerCount'),
            numSubscriptions = Sum('topic__subscriptionCount'),
            numConsumers  = Sum('topic__consumerCount'),
            backlog       = Sum('topic__backlog'),
            storage       = Sum('topic__storageSize'),
            rateIn        = Sum('topic__msgRateIn'),
            rateOut       = Sum('topic__msgRateOut'),
            throughputIn  = Sum('topic__msgThroughputIn'),
            throughputOut = Sum('topic__msgThroughputOut'),
        )

    namespaces = Table(request, namespaces, default_sort='name')

    return render(request, 'stats/property.html', {
        'property': property,
        'namespaces' : namespaces,
        'title' : property.name,
    })


def namespace(request, namespace_name):
    selectedClusterName = request.GET.get('cluster')

    namespace = get_object_or_404(Namespace, name=namespace_name, timestamp=get_timestamp(), deleted=False)
    topics = Topic.objects.select_related('broker', 'namespace', 'cluster')
    if selectedClusterName:
        topics = topics.filter(
                    namespace     = namespace,
                    timestamp     = get_timestamp(),
                    cluster__name = selectedClusterName,
                    deleted       = False
                )
    else:
        topics = topics.filter(
                    namespace = namespace,
                    timestamp = get_timestamp(),
                    deleted   = False
                )

    topics = Table(request, topics, default_sort='name')
    return render(request, 'stats/namespace.html', {
        'namespace' : namespace,
        'topics' : topics,
        'title' : namespace.name,
        'selectedCluster' : selectedClusterName,
    })


def delete_namespace(request, namespace_name):
    url = NamespaceName(namespace_name).admin_path()
    response = requests.delete(url)
    status = response.status_code
    logger.debug("Delete namespace " + namespace_name + " status - " + str(status))
    if status == 204:
        Namespace.objects.filter(name=namespace_name, timestamp=get_timestamp()).update(deleted=True)
    return redirect('property', property_name=namespace_name.split('/', 1)[0])


def topic(request, topic_name):
    timestamp = get_timestamp()
    topic_name = TopicName(topic_name).full_name()
    cluster_name = request.GET.get('cluster')
    clusters = []

    if cluster_name:
        topic = get_object_or_404(Topic, name=topic_name, cluster__name=cluster_name, timestamp=timestamp)
        clusters = [x.cluster for x in Topic.objects.filter(name=topic_name, timestamp=timestamp).order_by('cluster__name')]
    else:
        topic = get_object_or_404(Topic, name=topic_name, timestamp=timestamp)
    subscriptions = Subscription.objects.filter(topic=topic, timestamp=timestamp, deleted=False).order_by('name')

    subs = []

    for sub in subscriptions:
        consumers = Consumer.objects.filter(subscription=sub).order_by('address')
        subs.append((sub, consumers))

    if topic.is_global():
        peers_clusters = Replication.objects.filter(
                        timestamp = timestamp,
                        local_cluster__name = cluster_name
                    ).values('remote_cluster__name'
                    ).annotate(
                            Sum('msgRateIn'),
                            Sum('msgThroughputIn'),
                            Sum('msgRateOut'),
                            Sum('msgThroughputOut'),
                            Sum('replicationBacklog')
                    )
    else:
        peers_clusters = []

    return render(request, 'stats/topic.html', {
        'topic' : topic,
        'subscriptions' : subs,
        'title' : topic.name,
        'selectedCluster' : cluster_name,
        'clusters' : clusters,
        'peers' : peers_clusters,
    })

def topics(request):
    selectedClusterName = request.GET.get('cluster')

    topics = Topic.objects.select_related('broker', 'namespace', 'cluster')
    if selectedClusterName:
        topics = topics.filter(
                    timestamp     = get_timestamp(),
                    cluster__name = selectedClusterName,
                    deleted = False
                )
    else:
        topics = topics.filter(
                    timestamp = get_timestamp(),
                    deleted = False
                )

    topics = Table(request, topics, default_sort='cluster__name')
    return render(request, 'stats/topics.html', {
        'clusters' : Cluster.objects.all(),
        'topics' : topics,
        'title' : 'Topics',
        'selectedCluster' : selectedClusterName,
    })


def brokers(request):
    return brokers_cluster(request, None)

def brokers_cluster(request, cluster_name):
    ts = get_timestamp()

    brokers = Broker.objects
    if cluster_name:
        brokers = brokers.filter(
                    Q(topic__timestamp=ts) | Q(topic__timestamp__isnull=True),
                    activebroker__timestamp = ts,
                    cluster__name    = cluster_name,

                )
    else:
        brokers = brokers.filter(
                    Q(topic__timestamp=ts) | Q(topic__timestamp__isnull=True),
                    activebroker__timestamp = ts
                )

    brokers = brokers.annotate(
        numBundles       = Count('topic__bundle', True),
        numTopics        = Count('topic'),
        numProducers     = Sum('topic__producerCount'),
        numSubscriptions = Sum('topic__subscriptionCount'),
        numConsumers     = Sum('topic__consumerCount'),
        backlog          = Sum('topic__backlog'),
        storage          = Sum('topic__storageSize'),
        rateIn           = Sum('topic__msgRateIn'),
        rateOut          = Sum('topic__msgRateOut'),
        throughputIn     = Sum('topic__msgThroughputIn'),
        throughputOut    = Sum('topic__msgThroughputOut'),
    )

    brokers = Table(request, brokers, default_sort='url')

    return render(request, 'stats/brokers.html', {
        'clusters' : Cluster.objects.all(),
        'brokers' : brokers,
        'selectedCluster' : cluster_name,
    })

def broker(request, broker_url):
    broker = Broker.objects.get(url = broker_url)
    topics = Topic.objects.filter(
                timestamp = get_timestamp(),
                broker__url = broker_url
            )

    topics = Table(request, topics, default_sort='namespace__name')
    return render(request, 'stats/broker.html', {
        'topics' : topics,
        'title' : 'Broker - %s - %s' % (broker.cluster, broker_url),
        'broker_url' : broker_url
    })


def clusters(request):
    ts = get_timestamp()

    clusters = Cluster.objects.filter(
                    topic__timestamp = ts
                )

    clusters = clusters.annotate(
        numTopics           = Count('topic'),
        localBacklog        = Sum('topic__backlog'),
        replicationBacklog  = Sum('topic__replicationBacklog'),
        storage             = Sum('topic__storageSize'),
        localRateIn         = Sum('topic__localRateIn'),
        localRateOut        = Sum('topic__localRateOut'),
        replicationRateIn   = Sum('topic__replicationRateIn'),
        replicationRateOut  = Sum('topic__replicationRateOut'),
    )

    clusters = Table(request, clusters, default_sort='name')

    for cluster in clusters.results:
        # Fetch per-remote peer stats
        peers = Replication.objects.filter(
                        timestamp = ts,
                        local_cluster = cluster,
                    ).values('remote_cluster__name')

        peers = peers.annotate(
            Sum('msgRateIn'),
            Sum('msgThroughputIn'),
            Sum('msgRateOut'),
            Sum('msgThroughputOut'),
            Sum('replicationBacklog')
        )
        cluster.peers = peers

    return render(request, 'stats/clusters.html', {
        'clusters' : clusters,
    })


def clear_subscription(request, topic_name, subscription_name):
    url = "%s/subscription/%s/skip_all" % (TopicName(topic_name).admin_path(), subscription_name)
    response = requests.post(url)
    if response.status_code == 204:
        ts = get_timestamp()
        topic_db_name = TopicName(topic_name).full_name()
        topic = Topic.objects.get(name=topic_db_name, timestamp=ts)
        subscription = Subscription.objects.get(name=subscription_name, topic=topic, timestamp=ts)
        topic.backlog = topic.backlog - subscription.msgBacklog
        topic.save(update_fields=['backlog'])
        subscription.msgBacklog = 0
        subscription.save(update_fields=['msgBacklog'])
    return redirect('topic', topic_name=topic_name)


def delete_subscription(request, topic_name, subscription_name):
    url = "%s/subscription/%s" % (TopicName(topic_name).admin_path(), subscription_name)
    response = requests.delete(url)
    status = response.status_code
    if status == 204:
        ts = get_timestamp()
        topic_db_name = TopicName(topic_name).full_name()
        topic = Topic.objects.get(name=topic_db_name, timestamp=ts)
        deleted_subscription = Subscription.objects.get(name=subscription_name, topic=topic, timestamp=ts)
        deleted_subscription.deleted = True
        deleted_subscription.save(update_fields=['deleted'])
        subscriptions = Subscription.objects.filter(topic=topic, deleted=False, timestamp=ts)
        if not subscriptions:
            topic.deleted=True
            topic.save(update_fields=['deleted'])
            m = re.search(r"persistent/(?P<namespace>.*)/.*", topic_name)
            namespace_name = m.group("namespace")
            return redirect('namespace', namespace_name=namespace_name)
        else:
            topic.backlog = topic.backlog - deleted_subscription.msgBacklog
            topic.save(update_fields=['backlog'])
    return redirect('topic', topic_name=topic_name)


def messages(request, topic_name, subscription_name):
    topic_name_obj = TopicName(topic_name)
    topic_db_name = topic_name_obj.full_name()
    timestamp = get_timestamp()
    cluster_name = request.GET.get('cluster')

    if cluster_name:
        topic_obj = get_object_or_404(Topic,
                                      name=topic_db_name,
                                      cluster__name=cluster_name,
                                      timestamp=timestamp)
    else:
        topic_obj = get_object_or_404(Topic,
                                      name=topic_db_name, timestamp=timestamp)
    subscription_obj = get_object_or_404(Subscription,
                                         topic=topic_obj, name=subscription_name)

    message = None
    message_position = request.GET.get('message-position')
    if message_position and message_position.isnumeric():
        message = peek_message(topic_name_obj, subscription_name, message_position)
        if not isinstance(message, list):
            message = [message]

    return render(request, 'stats/messages.html', {
        'topic': topic_obj,
        'subscription': subscription_obj,
        'title': topic_obj.name,
        'subtitle': subscription_name,
        'message': message,
        'position': message_position or 1,
    })


def message_skip_meta(message_view):
    if not message_view or len(message_view) < 4:
        raise ValueError("invalid message")
    meta_size = struct.unpack(">I", message_view[:4])
    message_index = 4 + meta_size[0]
    if len(message_view) < message_index:
        raise ValueError("invalid message")
    return message_view[message_index:]


def parse_batch_message_supported():
    if pulsar is None:
        return False
    try:
        _ = pulsar.MessageBatch
        return True
    except AttributeError:
        return False


def parse_batch_message_entry(response, message_id, batch_size):
    message_id_parts = message_id.split(':')
    batch_message_id = pulsar.MessageId(-1,
                                        int(message_id_parts[0]),
                                        int(message_id_parts[1]),
                                        -1)
    return pulsar.MessageBatch()\
        .with_message_id(batch_message_id)\
        .parse_from(response.content, batch_size)


def format_message_metas(properties):
    return "Properties:\n%s" % json.dumps(properties,
                                          ensure_ascii=False, indent=2)


def format_single_message(message_id, properties, data):
    message = {
        message_id: format_message_metas(properties),
        "Hex": hexdump.hexdump(data, result='return'),
    }
    try:
        text = str(data,
                   encoding=chardet.detect(data)['encoding'],
                   errors='strict')
        message["Text"] = text
        message["JSON"] = json.dumps(json.loads(text),
                                     ensure_ascii=False, indent=2)
    except Exception:
        pass
    return message


def get_batch_message_from_http_response(response, message_id, batch_size):
    entries = parse_batch_message_entry(response, message_id, batch_size)
    message_list = []
    for entry in entries:
        single_msg_id = entry.message_id()
        single_msg_id = "%s:%s:%s" % (single_msg_id.ledger_id(),
                                      single_msg_id.entry_id(),
                                      single_msg_id.batch_index(),)
        message_list.append(format_single_message(single_msg_id,
                                                  entry.properties(),
                                                  entry.data()))
    return message_list


def get_properties_from_http_header(response):
    properties = {}
    for k, v in response.headers.items():
        if k.startswith("X-Pulsar-PROPERTY-"):
            properties[k.replace("X-Pulsar-PROPERTY-", "", 1)] = v
    return properties


def get_message_from_http_response(response):
    message_id = response.headers.get("X-Pulsar-Message-ID")
    if message_id is None:
        raise ValueError("invalid peek response")
    batch_size = response.headers.get('X-Pulsar-num-batch-message')
    if batch_size is not None:
        batch_size = int(batch_size)
        if parse_batch_message_supported():
            return get_batch_message_from_http_response(response, message_id, batch_size)
        else:
            if batch_size == 1:
                message_id = message_id + ":0"
                message_view = message_skip_meta(memoryview(response.content))
                return format_single_message(message_id,
                                             {},
                                             message_view.tobytes())
            else:
                return {"Batch": "(size=%d)<omitted>" % batch_size}
    else:
        get_properties_from_http_header(response)
        return format_single_message(message_id,
                                     get_properties_from_http_header(response),
                                     response.content)


def peek_message(topic_name, subscription_name, message_position):
    if not isinstance(topic_name, TopicName):
        topic_name = TopicName(topic_name)
    peek_url = "%s/subscription/%s/position/%s" % (
        topic_name.admin_path(), subscription_name, message_position)
    peek_response = requests.get(peek_url)
    if peek_response.status_code != 200:
        return {"ERROR": "%s(%d)" % (peek_response.reason, peek_response.status_code)}
    return get_message_from_http_response(peek_response)
