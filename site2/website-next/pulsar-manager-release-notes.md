## Apache Pulsar Manager

### 0.2.0 &mdash; 2020-09-28 <a id="0.2.0"></a>

* Support multiple addresses for the broker stats [PR-306](https://github.com/apache/pulsar-manager/pull/306).
* Use `PulsarAdmin` instead of `HttpUti`l in `BrokerStatsServiceImpl` [PR-315](https://github.com/apache/pulsar-manager/pull/315).
* Serve frontend directly from Pulsar Manager backend process [PR-288](https://github.com/apache/pulsar-manager/pull/288).
* Support docker for JWT [PR-218](https://github.com/apache/pulsar-manager/pull/218).
* Support sub and unsub operations [PR-240](https://github.com/apache/pulsar-manager/pull/240).
* Support peeking messages from the Pulsar broker [PR-241](https://github.com/apache/pulsar-manager/pull/241).
* Support BookKeeper visual manager 1.2.0 [PR-300](https://github.com/apache/pulsar-manager/pull/300).
* Support forwarding messages through HTTPS [PR-258](https://github.com/apache/pulsar-manager/pull/258).
* Support displaying stats for tenants and namespaces [PR-243](https://github.com/apache/pulsar-manager/pull/243).
* Add a configuration file for the backend service of Pulsar manager [PR-236](https://github.com/apache/pulsar-manager/pull/236).
* Add default configurations for the environment [PR-242](https://github.com/apache/pulsar-manager/pull/242).
* Fix an SQL syntax error [PR-298](https://github.com/apache/pulsar-manager/pull/298).
* Fix the issue that Pulsar Manager fail to process the request sent to the Pulsar proxy [PR-281](https://github.com/apache/pulsar-manager/pull/281).
* Change the default port and replace the request URI [PR-316](https://github.com/apache/pulsar-manager/pull/316).


### 0.1.0 &mdash; 2019-11-25 <a id="0.1.0"></a>

* Remove streamnative from the project [PR-213](https://github.com/apache/pulsar-manager/pull/213).
* Add license file for pulsar-manager [PR-209](https://github.com/apache/pulsar-manager/pull/209).
* Support management of jwt for pulsar-manager [PR-205](https://github.com/apache/pulsar-manager/pull/205).
* Support redirect.scheme [PR-204](https://github.com/apache/pulsar-manager/pull/204).
* Fix reset cursor by time [PR-179](https://github.com/apache/pulsar-manager/pull/179).
* Fix wrong broker display error [PR-187](https://github.com/apache/pulsar-manager/pull/187).
* Remove dependency package jszip [PR-189](https://github.com/apache/pulsar-manager/pull/189).
* Add developer guide [PR-186](https://github.com/apache/pulsar-manager/pull/186).
* Keep table and column name fields lowercase [PR-190](https://github.com/apache/pulsar-manager/pull/190).
* Fix loggin level [PR-191](https://github.com/apache/pulsar-manager/pull/191).
* Fix wrong place for license scan badge [PR-193](https://github.com/apache/pulsar-manager/pull/193).
* Add support for HerdDB database [PR-183](https://github.com/apache/pulsar-manager/pull/183).
* Make default environment persistent [PR-197](https://github.com/apache/pulsar-manager/pull/197).