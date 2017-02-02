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

# -*- coding: utf-8 -*-
# Generated by Django 1.10.5 on 2017-02-01 22:39
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Broker',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('url', models.URLField(db_index=True)),
            ],
        ),
        migrations.CreateModel(
            name='Bundle',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.BigIntegerField(db_index=True)),
                ('range', models.CharField(max_length=200)),
                ('broker', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Broker')),
            ],
        ),
        migrations.CreateModel(
            name='Cluster',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200, unique=True)),
                ('serviceUrl', models.URLField()),
            ],
        ),
        migrations.CreateModel(
            name='Consumer',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.BigIntegerField(db_index=True)),
                ('address', models.CharField(max_length=64, null=True)),
                ('availablePermits', models.IntegerField(default=0)),
                ('connectedSince', models.DateTimeField(null=True)),
                ('consumerName', models.CharField(max_length=64, null=True)),
                ('msgRateOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgRateRedeliver', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgThroughputOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('unackedMessages', models.BigIntegerField(default=0)),
                ('blockedConsumerOnUnackedMsgs', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='LatestTimestamp',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=10, unique=True)),
                ('timestamp', models.BigIntegerField(default=0)),
            ],
        ),
        migrations.CreateModel(
            name='Namespace',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200, unique=True)),
                ('clusters', models.ManyToManyField(to='stats.Cluster')),
            ],
        ),
        migrations.CreateModel(
            name='Property',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200, unique=True)),
            ],
            options={
                'verbose_name_plural': 'properties',
            },
        ),
        migrations.CreateModel(
            name='Replication',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.BigIntegerField(db_index=True)),
                ('msgRateIn', models.DecimalField(decimal_places=1, max_digits=12)),
                ('msgThroughputIn', models.DecimalField(decimal_places=1, max_digits=12)),
                ('msgRateOut', models.DecimalField(decimal_places=1, max_digits=12)),
                ('msgThroughputOut', models.DecimalField(decimal_places=1, max_digits=12)),
                ('msgRateExpired', models.DecimalField(decimal_places=1, max_digits=12)),
                ('replicationBacklog', models.BigIntegerField(default=0)),
                ('connected', models.BooleanField(default=False)),
                ('replicationDelayInSeconds', models.IntegerField(default=0)),
                ('inboundConnectedSince', models.DateTimeField(null=True)),
                ('outboundConnectedSince', models.DateTimeField(null=True)),
                ('local_cluster', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Cluster')),
                ('remote_cluster', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='remote_cluster', to='stats.Cluster')),
            ],
        ),
        migrations.CreateModel(
            name='Subscription',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=200)),
                ('timestamp', models.BigIntegerField(db_index=True)),
                ('msgBacklog', models.BigIntegerField(default=0)),
                ('msgRateExpired', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgRateOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgRateRedeliver', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgThroughputOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('subscriptionType', models.CharField(choices=[('N', 'Not connected'), ('E', 'Exclusive'), ('S', 'Shared'), ('F', 'Failover')], default='N', max_length=1)),
                ('unackedMessages', models.BigIntegerField(default=0)),
                ('namespace', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Namespace')),
            ],
        ),
        migrations.CreateModel(
            name='Topic',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(db_index=True, max_length=1024)),
                ('timestamp', models.BigIntegerField(db_index=True)),
                ('averageMsgSize', models.IntegerField(default=0)),
                ('msgRateIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgRateOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgThroughputIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('msgThroughputOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('pendingAddEntriesCount', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('producerCount', models.IntegerField(default=0)),
                ('subscriptionCount', models.IntegerField(default=0)),
                ('consumerCount', models.IntegerField(default=0)),
                ('storageSize', models.BigIntegerField(default=0)),
                ('backlog', models.BigIntegerField(default=0)),
                ('localRateIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('localRateOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('localThroughputIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('localThroughputOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('replicationRateIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('replicationRateOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('replicationThroughputIn', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('replicationThroughputOut', models.DecimalField(decimal_places=1, default=0, max_digits=12)),
                ('replicationBacklog', models.BigIntegerField(default=0)),
                ('broker', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Broker')),
                ('bundle', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Bundle')),
                ('cluster', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Cluster')),
                ('namespace', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Namespace')),
            ],
        ),
        migrations.AddField(
            model_name='subscription',
            name='topic',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Topic'),
        ),
        migrations.AddField(
            model_name='replication',
            name='topic',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Topic'),
        ),
        migrations.AddField(
            model_name='namespace',
            name='property',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Property'),
        ),
        migrations.AddField(
            model_name='consumer',
            name='subscription',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Subscription'),
        ),
        migrations.AddField(
            model_name='bundle',
            name='cluster',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Cluster'),
        ),
        migrations.AddField(
            model_name='bundle',
            name='namespace',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Namespace'),
        ),
        migrations.AddField(
            model_name='broker',
            name='cluster',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='stats.Cluster'),
        ),
        migrations.AlterIndexTogether(
            name='topic',
            index_together=set([('name', 'cluster', 'timestamp')]),
        ),
    ]
