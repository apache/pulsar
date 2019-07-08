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
from django.shortcuts import render, get_object_or_404, redirect
from django.template import loader
from django.urls import reverse
from django.views import generic
from django.db.models import Q, IntegerField
from dashboard import settings
import requests, json, re

from django.http import HttpResponseRedirect, HttpResponse
from .models import *

from stats.templatetags.table import Table
logger = logging.getLogger(__name__)
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
        ).annotate(
            numNamespaces = Subquery(
                Namespace.objects.filter(
                    deleted=False,
                    timestamp=ts,
                    property=OuterRef('pk')
                ).values('property')
                    .annotate(cnt=Count('pk'))
                    .values('cnt'),
                output_field=IntegerField()
            ),
            numTopics    = Count('namespace__topic__name', distinct=True),
            numProducers = Sum('namespace__topic__producerCount'),
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


def deleteNamespace(request, namespace_name):
    url = settings.SERVICE_URL + '/admin/v2/namespaces/' + namespace_name
    response = requests.delete(url)
    status = response.status_code
    logger.debug("Delete namespace " + namespace_name + " status - " + str(status))
    if status == 204:
        Namespace.objects.filter(name=namespace_name, timestamp=get_timestamp()).update(deleted=True)
    return redirect('property', property_name=namespace_name.split('/', 1)[0])

def topic(request, topic_name):
    timestamp = get_timestamp()
    topic_name = extract_topic_db_name(topic_name)
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


def clearSubscription(request, topic_name, subscription_name):
    url = settings.SERVICE_URL + '/admin/v2/' + topic_name + '/subscription/' + subscription_name + '/skip_all'
    response = requests.post(url)
    if response.status_code == 204:
        ts = get_timestamp()
        topic_db_name = extract_topic_db_name(topic_name)
        topic = Topic.objects.get(name=topic_db_name, timestamp=ts)
        subscription = Subscription.objects.get(name=subscription_name, topic=topic, timestamp=ts)
        topic.backlog = topic.backlog - subscription.msgBacklog
        topic.save(update_fields=['backlog'])
        subscription.msgBacklog = 0
        subscription.save(update_fields=['msgBacklog'])
    return redirect('topic', topic_name=topic_name)

def deleteSubscription(request, topic_name, subscription_name):
    url = settings.SERVICE_URL + '/admin/v2/' + topic_name + '/subscription/' + subscription_name
    response = requests.delete(url)
    status = response.status_code
    if status == 204:
        ts = get_timestamp()
        topic_db_name = extract_topic_db_name(topic_name)
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
    topic_name = extract_topic_db_name(topic_name)
    timestamp = get_timestamp()
    cluster_name = request.GET.get('cluster')

    if cluster_name:
        topic = get_object_or_404(Topic, name=topic_name, cluster__name=cluster_name, timestamp=timestamp)
    else:
        topic = get_object_or_404(Topic, name=topic_name, timestamp=timestamp)
    subscription = get_object_or_404(Subscription, topic=topic, name=subscription_name)

    return render(request, 'stats/messages.html', {
        'topic' : topic,
        'subscription' : subscription,
        'title' : topic.name,
        'subtitle' : subscription_name,
    })

def peek(request, topic_name, subscription_name, message_number):
    url = settings.SERVICE_URL + '/admin/v2/' + topic_name + '/subscription/' + subscription_name + '/position/' + message_number
    response = requests.get(url)
    message = response.text
    message = message[message.index('{'):]
    context = {
        'message_body' : json.dumps(json.loads(message), indent=4),
    }
    return render(request, 'stats/peek.html', context)


def extract_topic_db_name(topic_name):
    return 'persistent://' + topic_name.split('persistent/', 1)[1]