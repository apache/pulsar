---
id: io-http-sink
title: HTTTP sink connector
sidebar_label: "HTTP sink connector"
---

The HTTP sink connector pulls the records from Pulsar topics and makes a POST request to a configurable HTTP URL (webhook).

The body of the HTTP request is the JSON representation of the record value. The header `Content-Type: application/json` is added to the HTTP request.

Some other HTTP headers are added to the HTTP request:

* `PulsarTopic`: the topic of the record
* `PulsarKey`: the key of the record
* `PulsarEventTime`: the event time of the record
* `PulsarPublishTime`: the publish time of the record
* `PulsarMessageId`: the ID of the message contained in the record
* `PulsarProperties-*`: each record property is passed with the property name prefixed by `PulsarProperties-`

## Configuration

The configuration of the HTTP sink connector has the following properties.

### Property

| Name      | Type   | Required | Default          | Description                                       |
|-----------|--------|----------|------------------|---------------------------------------------------|
| `url`     | String | false    | http://localhost | The URL of the HTTP server                        |
| `headers` | Map    | false    | empty map        | The list of default headers added to each request |

### Example

Before using the HTTP sink connector, you need to create a configuration file through one of the following methods.

* JSON 

  ```json
  {
     "configs": {
        "url": "http://my-endpoint.acme.com/api/ingest",
        "headers": {
           "Authentication": "xxxxx"
        }
     }
  }
  ```

* YAML

  ```yaml
  configs:
      url: "http://my-endpoint.acme.com/api/ingest"
      headers:
          Authentication: xxxxx
  ```

