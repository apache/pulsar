#
# Copyright 2016 Yahoo Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from django.shortcuts import render, get_object_or_404
from django.template import loader
from django.urls import reverse
from django.views import generic
from django.db.models import Q

from django.http import HttpResponseRedirect, HttpResponse
from .models import *

from stats.templatetags.table import Table

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

    print properties.query

    properties = Table(request, properties, default_sort='name')

    return render(request, 'stats/home.html', {
        'properties': properties,
        'title' : 'Properties',
    })

def property(request, property_name):
    property = get_object_or_404(Property, name=property_name)
    ts = get_timestamp()
    namespaces = Namespace.objects.filter(
            property = property,
            topic__timestamp = ts,
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

    namespace = get_object_or_404(Namespace, name=namespace_name)
    topics = Topic.objects.select_related('broker', 'namespace', 'cluster')
    if selectedClusterName:
        topics = topics.filter(
                    namespace     = namespace,
                    timestamp     = get_timestamp(),
                    cluster__name = selectedClusterName
                )
    else:
        topics = topics.filter(
                    namespace = namespace,
                    timestamp = get_timestamp()
                )

    topics = Table(request, topics, default_sort='name')
    return render(request, 'stats/namespace.html', {
        'namespace' : namespace,
        'topics' : topics,
        'title' : namespace.name,
        'selectedCluster' : selectedClusterName,
    })


def topic(request, topic_name):
    timestamp = get_timestamp()
    topic_name = 'persistent://' + topic_name.split('persistent/', 1)[1]
    cluster_name = request.GET.get('cluster')
    clusters = []

    if cluster_name:
        topic = get_object_or_404(Topic, name=topic_name, cluster__name=cluster_name, timestamp=timestamp)
        clusters = [x.cluster for x in Topic.objects.filter(name=topic_name, timestamp=timestamp).order_by('cluster__name')]
    else:
        topic = get_object_or_404(Topic, name=topic_name, timestamp=timestamp)
    subscriptions = Subscription.objects.filter(topic=topic).order_by('name')

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
                    cluster__name = selectedClusterName
                )
    else:
        topics = topics.filter(
                    timestamp = get_timestamp()
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
        numBundles       = Count('topic__bundle'),
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
