---
title: Pulsar 2.3
hide_pulsar2_notification: true
new: true
tags: ["2.3"]
---

Pulsar 2.3 is a major new release for Pulsar. In this release, we have greatly improved the user experience for Pulsar Functions, Sources, and Sinks by redesigning and some of the REST and admin APIs and also adding new ones.
## New Features 


## Major Changes

There are a few major changes that you should be aware of, as they may significantly impact your day-to-day usage.

### Admin API for Functions, Sources, Sinks, and Worker

<b>Please note: if you are planning using the admin API or CLI for Functions, please update Admin Client to 2.3 because there are some changes break backwards compatibility!</b>

A new set of admin APIs and REST endpoints have been added for Sources and Sinks to ensure a cleaner API.  Prior to 2.3, Sources and Sinks were submitted via the same REST endpoints as Functions.  Using a  single REST endpoint for submitting Sinks, Sources, and Functions was complex and not user friendly. Thus, we made the decision to separate out the REST endpoints for Sinks and Sources so that the parameters needed for submitting a Sink, Source, or Function are much clearer. 

Also, prior to 2.3, Sources and Sinks did not have their own admin API.  It 2.3 we have added a set of admin APIs dedicated for sources and sinks

```bash
pulsarAdmin.source();
pulsarAdmin.sink();
```

Please use these set of endpoints to interact with Sources and Sinks, respectively.

<b>Please note:  using the Functions admin API / Functions CLI to interact with Sources and Sinks will not longer work in 2.3!</b>

Many methods in the admin APIs and corresponding REST endpoints for Functions and Workers have been redesigned to improve user experience. 

Methods have not been removed, only return type has changed to accommodate a better user experience

Below is a list of methods that have been changed:

<b>Worker Admin API</b>

| 2.2 | 2.3 |
|-----|-----|
|```Metrics getFunctionsStats() throws PulsarAdminException;```| ```List<WorkerFunctionInstanceStats> getFunctionsStats() throws PulsarAdminException;```|


<b>Functions Admin API</b>

| 2.2 | 2.3 |
|-----|-----|
|```FunctionDetails getFunction(String tenant, String namespace, String function) throws PulsarAdminException;```| ```FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException;```|
|``` void createFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException;```|``` void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException;```|
|```void updateFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException;```|```void updateFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException;```|
|```void updateFunctionWithUrl(FunctionDetails functionDetails, String pkgUrl) throws PulsarAdminException;```|```void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException;```|
|```FunctionStatusList getFunctionStatus(String tenant, String namespace, String function) throws PulsarAdminException;```|```FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionStatus(String tenant, String namespace, String function, int id) throws PulsarAdminException;```|
|```String getFunctionState(String tenant, String namespace, String function, String key) throws PulsarAdminException;```|```FunctionState getFunctionState(String tenant, String namespace, String function, String key) throws PulsarAdminException;```|
