#!/usr/bin/env python
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


import os
import django
import requests
import pytz
import multiprocessing
import traceback
import sys
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db import connection
import time
import argparse

current_milli_time = lambda: int(round(time.time() * 1000))

def get(base_url, path):
    if base_url.endswith('/'): path = path[1:]
    return requests.get(base_url + path,
            headers=http_headers,
            proxies=http_proxyes,
        ).json()

def parse_date(d):
    if d:
        dt = parse_datetime(d)
        if dt.tzinfo:
            # There is already the timezone set
            return dt
        else:
            # Assume UTC if no timezone
            return pytz.timezone('UTC').localize(parse_datetime(d))
    else: return None

# Fetch the stats for a given broker
def fetch_broker_stats(cluster, broker_url, timestamp):
    try:
        _fetch_broker_stats(cluster, broker_url, timestamp)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise e


def _fetch_broker_stats(cluster, broker_host_port, timestamp):
    broker_url = 'http://%s/' % broker_host_port
    print '    Getting stats for %s' % broker_host_port

    broker, _ = Broker.objects.get_or_create(
                        url     = broker_host_port,
                        cluster = cluster
                )
    active_broker = ActiveBroker(broker=broker, timestamp=timestamp)
    active_broker.save()

    # Get topics stats
    topics_stats = get(broker_url, '/admin/v2/broker-stats/destinations')

    clusters = dict( (cluster.name, cluster) for cluster in Cluster.objects.all() )

    db_bundles = []
    db_topics = []
    db_subscriptions = []
    db_consumers = []
    db_replication = []

    for namespace_name, bundles_stats in topics_stats.items():
        property_name = namespace_name.split('/')[0]
        property, _ = Property.objects.get_or_create(name=property_name)

        namespace, _ = Namespace.objects.get_or_create(
                            name=namespace_name,
                            property=property)
        namespace.clusters.add(cluster)
        namespace.save()

        for bundle_range, topics_stats in bundles_stats.items():
            bundle = Bundle(
                            broker    = broker,
                            namespace = namespace,
                            range     = bundle_range,
                            cluster   = cluster,
                            timestamp = timestamp)
            db_bundles.append(bundle)

            for topic_name, stats in topics_stats['persistent'].items():
                topic = Topic(
                    broker                 = broker,
                    active_broker          = active_broker,
                    name                   = topic_name,
                    namespace              = namespace,
                    bundle                 = bundle,
                    cluster                = cluster,
                    timestamp              = timestamp,
                    averageMsgSize         = stats['averageMsgSize'],
                    msgRateIn              = stats['msgRateIn'],
                    msgRateOut             = stats['msgRateOut'],
                    msgThroughputIn        = stats['msgThroughputIn'],
                    msgThroughputOut       = stats['msgThroughputOut'],
                    pendingAddEntriesCount = stats['pendingAddEntriesCount'],
                    producerCount          = stats['producerCount'],
                    storageSize            = stats['storageSize']
                )

                db_topics.append(topic)
                totalBacklog = 0
                numSubscriptions = 0
                numConsumers = 0

                for subscription_name, subStats in stats['subscriptions'].items():
                    numSubscriptions += 1
                    subscription = Subscription(
                        topic            = topic,
                        name             = subscription_name,
                        namespace        = namespace,
                        timestamp        = timestamp,
                        msgBacklog       = subStats['msgBacklog'],
                        msgRateExpired   = subStats['msgRateExpired'],
                        msgRateOut       = subStats['msgRateOut'],
                        msgRateRedeliver = subStats.get('msgRateRedeliver', 0),
                        msgThroughputOut = subStats['msgThroughputOut'],
                        subscriptionType = subStats['type'][0],
                        unackedMessages  = subStats.get('unackedMessages', 0),
                    )
                    db_subscriptions.append(subscription)

                    totalBacklog += subStats['msgBacklog']

                    for consStats in subStats['consumers']:
                        numConsumers += 1
                        consumer = Consumer(
                            subscription     = subscription,
                            timestamp        = timestamp,
                            address          = consStats['address'],
                            availablePermits = consStats.get('availablePermits', 0),
                            connectedSince   = parse_date(consStats.get('connectedSince')),
                            consumerName     = consStats.get('consumerName'),
                            msgRateOut       = consStats.get('msgRateOut', 0),
                            msgRateRedeliver = consStats.get('msgRateRedeliver', 0),
                            msgThroughputOut = consStats.get('msgThroughputOut', 0),
                            unackedMessages  = consStats.get('unackedMessages', 0),
                            blockedConsumerOnUnackedMsgs  = consStats.get('blockedConsumerOnUnackedMsgs', False)
                        )
                        db_consumers.append(consumer)

                topic.backlog = totalBacklog
                topic.subscriptionCount = numSubscriptions
                topic.consumerCount = numConsumers

                replicationMsgIn = 0
                replicationMsgOut = 0
                replicationThroughputIn = 0
                replicationThroughputOut = 0
                replicationBacklog = 0

                for remote_cluster, replStats in stats['replication'].items():
                    replication = Replication(
                        timestamp              = timestamp,
                        topic                  = topic,
                        local_cluster          = cluster,
                        remote_cluster         = clusters[remote_cluster],

                        msgRateIn              = replStats['msgRateIn'],
                        msgRateOut             = replStats['msgRateOut'],
                        msgThroughputIn        = replStats['msgThroughputIn'],
                        msgThroughputOut       = replStats['msgThroughputOut'],
                        replicationBacklog     = replStats['replicationBacklog'],
                        connected              = replStats['connected'],
                        replicationDelayInSeconds = replStats['replicationDelayInSeconds'],
                        msgRateExpired         = replStats['msgRateExpired'],

                        inboundConnectedSince  = parse_date(replStats.get('inboundConnectedSince')),
                        outboundConnectedSince = parse_date(replStats.get('outboundConnectedSince')),
                    )

                    db_replication.append(replication)

                    replicationMsgIn         += replication.msgRateIn
                    replicationMsgOut        += replication.msgRateOut
                    replicationThroughputIn  += replication.msgThroughputIn
                    replicationThroughputOut += replication.msgThroughputOut
                    replicationBacklog       += replication.replicationBacklog

                topic.replicationRateIn = replicationMsgIn
                topic.replicationRateOut = replicationMsgOut
                topic.replicationThroughputIn = replicationThroughputIn
                topic.replicationThroughputOut = replicationThroughputOut
                topic.replicationBacklog = replicationBacklog
                topic.localRateIn = topic.msgRateIn - replicationMsgIn
                topic.localRateOut = topic.msgRateOut - replicationMsgOut
                topic.localThroughputIn = topic.msgThroughputIn - replicationThroughputIn
                topic.localThroughputOut = topic.msgThroughputIn - replicationThroughputOut


    if connection.vendor == 'postgresql':
        # Bulk insert into db
        Bundle.objects.bulk_create(db_bundles, batch_size=10000)

        # Trick to refresh primary keys after previous bulk import
        for topic in db_topics: topic.bundle = topic.bundle
        Topic.objects.bulk_create(db_topics, batch_size=10000)

        for subscription in db_subscriptions: subscription.topic = subscription.topic
        Subscription.objects.bulk_create(db_subscriptions, batch_size=10000)

        for consumer in db_consumers: consumer.subscription = consumer.subscription
        Consumer.objects.bulk_create(db_consumers, batch_size=10000)

        for replication in db_replication: replication.topic = replication.topic
        Replication.objects.bulk_create(db_replication, batch_size=10000)

    else:
        # For other DB providers we have to insert one by one
        # to be able to retrieve the PK of the newly inserted records
        for bundle in db_bundles:
            bundle.save()

        for topic in db_topics:
            topic.bundle = topic.bundle
            topic.save()

        for subscription in db_subscriptions:
            subscription.topic = subscription.topic
            subscription.save()

        for consumer in db_consumers:
            consumer.subscription = consumer.subscription
            consumer.save()

        for replication in db_replication:
            replication.topic = replication.topic
            replication.save()


def fetch_stats():
    timestamp = current_milli_time()

    pool = multiprocessing.Pool(args.workers)

    futures = []

    for cluster_name in get(args.serviceUrl, '/admin/v2/clusters'):
        if cluster_name == 'global': continue

        cluster_url = get(args.serviceUrl, '/admin/v2/clusters/' + cluster_name)['serviceUrl']
        print 'Cluster:', cluster_name,  '->', cluster_url
        cluster, created = Cluster.objects.get_or_create(name=cluster_name)
        if cluster_url != cluster.serviceUrl:
            cluster.serviceUrl = cluster_url
            cluster.save()

    # Get the list of brokers for each cluster
    for cluster in Cluster.objects.all():
        try:
            for broker_host_port in get(cluster.serviceUrl, '/admin/v2/brokers/' + cluster.name):
                f = pool.apply_async(fetch_broker_stats, (cluster, broker_host_port, timestamp))
                futures.append(f)
        except Exception as e:
            print 'ERROR: ', e

    pool.close()

    for f in futures:
        f.get()

    pool.join()

    # Update Timestamp in DB
    latest, _ = LatestTimestamp.objects.get_or_create(name='latest')
    latest.timestamp = timestamp
    latest.save()

def purge_db():
    now = current_milli_time()
    ttl_minutes = args.purge
    threshold = now - (ttl_minutes * 60 * 1000)

    Bundle.objects.filter(timestamp__lt = threshold).delete()
    Topic.objects.filter(timestamp__lt = threshold).delete()
    Subscription.objects.filter(timestamp__lt = threshold).delete()
    Consumer.objects.filter(timestamp__lt = threshold).delete()

def collect_and_purge():
    print '-- Starting stats collection'
    fetch_stats()
    purge_db()
    print '-- Finished collecting stats'

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dashboard.settings")
    django.setup()

    from stats.models import *

    parser = argparse.ArgumentParser(description='Pulsar Stats collector')
    parser.add_argument(action="store", dest="serviceUrl", help='Service URL of one cluster in the Pulsar instance')

    parser.add_argument('--proxy', action='store',
                            help="Connect using a HTTP proxy", dest="proxy")
    parser.add_argument('--header', action="append", dest="header",
                            help='Add an additional HTTP header to all requests')
    parser.add_argument('--purge', action="store", dest="purge", type=int, default=60,
                            help='Purge statistics older than PURGE minutes. (default: 60min)')

    parser.add_argument('--workers', action="store", dest="workers", type=int, default=64,
                            help='Number of worker processes to be used to fetch the stats (default: 64)')

    global args
    args = parser.parse_args(sys.argv[1:])

    global http_headers
    http_headers = {}
    if args.header:
        http_headers = dict(x.split(': ') for x in args.header)

    global http_proxyes
    http_proxyes = {}
    if args.proxy:
        http_proxyes['http'] = args.proxy
        http_proxyes['https'] = args.proxy

    # Schedule a new collection every 1min
    while True:
        p = multiprocessing.Process(target=collect_and_purge)
        p.start()
        time.sleep(60)
