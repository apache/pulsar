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

from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^property/(?P<property_name>.+)/$', views.property, name='property'),
    url(r'^namespace/(?P<namespace_name>.+)/$', views.namespace, name='namespace'),
    url(r'^deleteNamespace/(?P<namespace_name>.+)$', views.delete_namespace, name='deleteNamespace'),


    url(r'^brokers/$', views.brokers, name='brokers'),
    url(r'^brokers/(?P<cluster_name>.+)/$', views.brokers_cluster, name='brokers_cluster'),
    url(r'^broker/(?P<broker_url>.+)/$', views.broker, name='broker'),

    url(r'^topics/$', views.topics, name='topics'),
    url(r'^topic/(?P<topic_name>.+)/$', views.topic, name='topic'),

    url(r'^clusters/$', views.clusters, name='clusters'),
    url(r'^clearSubscription/(?P<topic_name>.+)/(?P<subscription_name>.+)$', views.clear_subscription, name='clearSubscription'),
    url(r'^deleteSubscription/(?P<topic_name>.+)/(?P<subscription_name>.+)$', views.delete_subscription, name='deleteSubscription'),
    url(r'^messages/(?P<topic_name>.+)/(?P<subscription_name>.+)$', views.messages, name='messages'),
]
