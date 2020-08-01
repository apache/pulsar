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


import logging
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
import random

current_milli_time = lambda: int(round(time.time() * 1000))
logger = logging.getLogger(__name__)

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
    else:
        return None


# Fetch the stats for a given broker
def fetch_broker_stats(cluster, broker_url, timestamp):
    try:
        _fetch_broker_stats(cluster, broker_url, timestamp)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise e


def _fetch_broker_stats(cluster, broker_host_port, timestamp):
    broker_url = 'http://%s/' % broker_host_port
    logger.info('Getting stats for %s' % broker_host_port)

    broker, _ = Broker.objects.get_or_create(
        url=broker_host_port,
        cluster=cluster
    )
    active_broker = ActiveBroker(broker=broker, timestamp=timestamp)
    active_broker.save()

    # Get topics stats
    topics_stats = get(broker_url, '/admin/v2/broker-stats/topics')

    clusters = dict((cluster.name, cluster) for cluster in Cluster.objects.all())

    db_create_bundles = []
    db_update_bundles = []
    db_create_topics = []
    db_update_topics = []
    db_create_subscriptions = []
    db_update_subscriptions = []
    db_create_consumers = []
    db_update_consumers = []
    db_replication = []

    for namespace_name, bundles_stats in topics_stats.items():
        property_name = namespace_name.split('/')[0]
        property, _ = Property.objects.get_or_create(name=property_name)

        namespace, _ = Namespace.objects.get_or_create(
            name=namespace_name,
            property=property,
            timestamp=timestamp)
        namespace.clusters.add(cluster)
        namespace.save()

        for bundle_range, topics_stats in bundles_stats.items():

            bundle = Bundle.objects.filter(
                cluster_id=cluster.id,
                namespace_id=namespace.id,
                range=bundle_range)
            if bundle:
                temp_bundle = bundle.first()
                temp_bundle.timestame = timestamp
                db_update_bundles.append(temp_bundle)
                bundle = temp_bundle
            else:
                bundle = Bundle(
                    broker=broker,
                    namespace=namespace,
                    range=bundle_range,
                    cluster=cluster,
                    timestamp=timestamp)
                db_create_bundles.append(bundle)

            for topic_name, stats in topics_stats['persistent'].items():
                topic = Topic.objects.filter(
                    cluster_id=cluster.id,
                    bundle_id=bundle.id,
                    namespace_id=namespace.id,
                    broker_id=broker.id,
                    name=topic_name)
                if topic:
                    temp_topic = topic.first()
                    temp_topic.timestamp = timestamp
                    temp_topic.averageMsgSize = stats['averageMsgSize']
                    temp_topic.msgRateIn = stats['msgRateIn']
                    temp_topic.msgRateOut = stats['msgRateOut']
                    temp_topic.msgThroughputIn = stats['msgThroughputIn']
                    temp_topic.msgThroughputOut = stats['msgThroughputOut']
                    temp_topic.pendingAddEntriesCount = stats['pendingAddEntriesCount']
                    temp_topic.producerCount = stats['producerCount']
                    temp_topic.storageSize = stats['storageSize']
                    db_update_topics.append(temp_topic)
                    topic = temp_topic
                else:
                    topic = Topic(
                        broker=broker,
                        active_broker=active_broker,
                        name=topic_name,
                        namespace=namespace,
                        bundle=bundle,
                        cluster=cluster,
                        timestamp=timestamp,
                        averageMsgSize=stats['averageMsgSize'],
                        msgRateIn=stats['msgRateIn'],
                        msgRateOut=stats['msgRateOut'],
                        msgThroughputIn=stats['msgThroughputIn'],
                        msgThroughputOut=stats['msgThroughputOut'],
                        pendingAddEntriesCount=stats['pendingAddEntriesCount'],
                        producerCount=stats['producerCount'],
                        storageSize=stats['storageSize']
                    )
                    db_create_topics.append(topic)
                totalBacklog = 0
                numSubscriptions = 0
                numConsumers = 0

                for subscription_name, subStats in stats['subscriptions'].items():
                    numSubscriptions += 1
                    subscription = Subscription.objects.filter(
                        topic_id=topic.id,
                        namespace_id=namespace.id,
                        name=subscription_name)
                    if subscription:
                        temp_subscription = subscription.first()
                        temp_subscription.timestamp = timestamp
                        temp_subscription.msgBacklog = subStats['msgBacklog']
                        temp_subscription.msgRateExpired = subStats['msgRateExpired']
                        temp_subscription.msgRateOut = subStats['msgRateOut']
                        temp_subscription.msgRateRedeliver = subStats.get('msgRateRedeliver', 0)
                        temp_subscription.msgThroughputOut = subStats['msgThroughputOut']
                        temp_subscription.subscriptionType = subStats['type'][0]
                        temp_subscription.unackedMessages = subStats.get('unackedMessages', 0)
                        db_update_subscriptions.append(temp_subscription)
                        subscription = temp_subscription
                    else:
                        subscription = Subscription(
                            topic=topic,
                            name=subscription_name,
                            namespace=namespace,
                            timestamp=timestamp,
                            msgBacklog=subStats['msgBacklog'],
                            msgRateExpired=subStats['msgRateExpired'],
                            msgRateOut=subStats['msgRateOut'],
                            msgRateRedeliver=subStats.get('msgRateRedeliver', 0),
                            msgThroughputOut=subStats['msgThroughputOut'],
                            subscriptionType=subStats['type'][0],
                            unackedMessages=subStats.get('unackedMessages', 0),
                        )
                        db_create_subscriptions.append(subscription)

                    totalBacklog += subStats['msgBacklog']

                    for consStats in subStats['consumers']:
                        numConsumers += 1
                        consumer = Consumer.objects.filter(
                            subscription_id=subscription.id,
                            consumerName=consStats.get('consumerName'))
                        if consumer:
                            temp_consumer = consumer.first()
                            temp_consumer.timestamp = timestamp
                            temp_consumer.address = consStats['address']
                            temp_consumer.availablePermits = consStats.get('availablePermits', 0)
                            temp_consumer.connectedSince = parse_date(consStats.get('connectedSince'))
                            temp_consumer.msgRateOut = consStats.get('msgRateOut', 0)
                            temp_consumer.msgRateRedeliver = consStats.get('msgRateRedeliver', 0)
                            temp_consumer.msgThroughputOut = consStats.get('msgThroughputOut', 0)
                            temp_consumer.unackedMessages = consStats.get('unackedMessages', 0)
                            temp_consumer.blockedConsumerOnUnackedMsgs = consStats.get('blockedConsumerOnUnackedMsgs',
                                                                                       False)
                            db_update_consumers.append(temp_consumer)
                            consumer = temp_consumer
                        else:
                            consumer = Consumer(
                                subscription=subscription,
                                timestamp=timestamp,
                                address=consStats['address'],
                                availablePermits=consStats.get('availablePermits', 0),
                                connectedSince=parse_date(consStats.get('connectedSince')),
                                consumerName=consStats.get('consumerName'),
                                msgRateOut=consStats.get('msgRateOut', 0),
                                msgRateRedeliver=consStats.get('msgRateRedeliver', 0),
                                msgThroughputOut=consStats.get('msgThroughputOut', 0),
                                unackedMessages=consStats.get('unackedMessages', 0),
                                blockedConsumerOnUnackedMsgs=consStats.get('blockedConsumerOnUnackedMsgs', False)
                            )
                            db_create_consumers.append(consumer)

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
                        timestamp=timestamp,
                        topic=topic,
                        local_cluster=cluster,
                        remote_cluster=clusters[remote_cluster],

                        msgRateIn=replStats['msgRateIn'],
                        msgRateOut=replStats['msgRateOut'],
                        msgThroughputIn=replStats['msgThroughputIn'],
                        msgThroughputOut=replStats['msgThroughputOut'],
                        replicationBacklog=replStats['replicationBacklog'],
                        connected=replStats['connected'],
                        replicationDelayInSeconds=replStats['replicationDelayInSeconds'],
                        msgRateExpired=replStats['msgRateExpired'],

                        inboundConnectedSince=parse_date(replStats.get('inboundConnectedSince')),
                        outboundConnectedSince=parse_date(replStats.get('outboundConnectedSince')),
                    )

                    db_replication.append(replication)

                    replicationMsgIn += replication.msgRateIn
                    replicationMsgOut += replication.msgRateOut
                    replicationThroughputIn += replication.msgThroughputIn
                    replicationThroughputOut += replication.msgThroughputOut
                    replicationBacklog += replication.replicationBacklog

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
        Bundle.objects.bulk_create(db_create_bundles, batch_size=10000)

        # Trick to refresh primary keys after previous bulk import
        for topic in db_create_topics: topic.bundle = topic.bundle
        Topic.objects.bulk_create(db_create_topics, batch_size=10000)

        for subscription in db_create_subscriptions: subscription.topic = subscription.topic
        Subscription.objects.bulk_create(db_create_subscriptions, batch_size=10000)

        for consumer in db_create_consumers: consumer.subscription = consumer.subscription
        Consumer.objects.bulk_create(db_create_consumers, batch_size=10000)

        for replication in db_replication: replication.topic = replication.topic
        Replication.objects.bulk_create(db_replication, batch_size=10000)

        update_or_create_object(
            db_update_bundles,
            db_update_topics,
            db_update_consumers,
            db_update_subscriptions)

    else:
        update_or_create_object(
            db_create_bundles,
            db_create_topics,
            db_create_consumers,
            db_create_subscriptions)
        update_or_create_object(
            db_update_bundles,
            db_update_topics,
            db_update_consumers,
            db_update_subscriptions)

        for replication in db_replication:
            replication.topic = replication.topic
            replication.save()

    tenants = get(broker_url, '/admin/v2/tenants')
    for tenant_name in tenants:
        namespaces = get(broker_url, '/admin/v2/namespaces/' + tenant_name)
        for namespace_name in namespaces:
            property, _ = Property.objects.get_or_create(name=tenant_name)
            namespace, _ = Namespace.objects.get_or_create(
                name=namespace_name,
                property=property,
                timestamp=timestamp)
            namespace.clusters.add(cluster)
            namespace.save()


def update_or_create_object(db_bundles, db_topics, db_consumers, db_subscriptions):
    # For DB providers we have to insert or update one by one
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


def fetch_stats():
    logger.info("Begin fetch stats")
    timestamp = current_milli_time()

    pool = multiprocessing.Pool(args.workers)

    futures = []

    for cluster_name in get(args.serviceUrl, '/admin/v2/clusters'):
        if cluster_name == 'global': continue

        cluster_url = get(args.serviceUrl, '/admin/v2/clusters/' + cluster_name)['serviceUrl']
        if cluster_url.find(',')>=0:
            cluster_url_list = cluster_url.split(',')
            index = random.randint(0,len(cluster_url_list)-1)
            if index==0:
                cluster_url = cluster_url_list[index]
            else:
                protocol = ("https://" if(cluster_url.find("https")>=0) else "http://")
                cluster_url = protocol+cluster_url_list[index]

        logger.info('Cluster:{} -> {}'.format(cluster_name, cluster_url))
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
            logger.error('ERROR: ', e)

    pool.close()

    for f in futures:
        f.get()

    pool.join()

    # Update Timestamp in DB
    latest, _ = LatestTimestamp.objects.get_or_create(name='latest')
    latest.timestamp = timestamp
    latest.save()
    logger.info("Finished fetch stats")


def purge_db():
    logger.info("Begin purge db")
    now = current_milli_time()
    ttl_minutes = args.purge
    threshold = now - (ttl_minutes * 60 * 1000)

    Bundle.objects.filter(timestamp__lt=threshold).delete()
    Topic.objects.filter(timestamp__lt=threshold).delete()
    Subscription.objects.filter(timestamp__lt=threshold).delete()
    Consumer.objects.filter(timestamp__lt=threshold).delete()
    Namespace.objects.filter(timestamp__lt=threshold).delete()
    logger.info("Finished purge db")


def collect_and_purge():
    logger.info('Starting stats collection')
    fetch_stats()
    purge_db()
    logger.info('Finished collecting stats')


if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dashboard.settings")
    django.setup()

    from stats.models import *

    parser = argparse.ArgumentParser(description='Pulsar Stats collector')
    parser.add_argument(action="store", dest="serviceUrl", help='Service URL of one cluster in the Pulsar instance')

    parser.add_argument('--proxy', action='store',
                        help="Connect using an HTTP proxy", dest="proxy")
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
    jwt_token = os.getenv("JWT_TOKEN", None)
    if jwt_token is not None:
        http_headers = {
            "Authorization": "Bearer {}".format(jwt_token)}
    if args.header:
        http_headers = dict(x.split(': ') for x in args.header)
        logger.info(http_headers)

    global http_proxyes
    http_proxyes = { "no_proxy": os.getenv("NO_PROXY", "") }
    if args.proxy:
        http_proxyes['http'] = args.proxy
        http_proxyes['https'] = args.proxy

    # Schedule a new collection every 1min
    while True:
        p = multiprocessing.Process(target=collect_and_purge)
        p.start()
        time.sleep(int(os.getenv("COLLECTION_INTERVAL", 60)))
