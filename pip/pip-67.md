# PIP-67: Pulsarctl - An alternative tools of pulsar-admin

- Status: Proposal
- Author: Yong Zhang, Xiaolong Ran
- Pull Request: 
- Mailing List discussion:


## Motivation

Pulsarctl is an alternative tool of pulsar-admin, used to manage resources in Apache Pulsar. Pulsarctl is written in Go and based on Pulsar REST API. It provides Go developers with API interface and user-friendly commands, making it easier to interact with Pulsar brokers.

Compared with pulsar-admin, Pulsarctl is more user-friendly. Pulsarctl requires fewer dependencies to use commands and provides more comprehensive description and usage for commands. With Pulsarctl, users can find and resolve issues faster when errors occur. 

## Features

Pulsarctl not only integrates the Pulsar commands and BookKeeper commands but also provides some useful tools like output format and context management. 


### Pulsar operations

For Pulsar operations, Pulsarctl integrates almost all the commands of pulsar-admin, including but not limited to the following operations:

- broker operations
- cluster operations
- tenant operations
- namespace operations
- topic operations
- function operations
- sink operations
- source operations

Also, Pulsarctl supports the JWT(JSON Web Token) authentication and TLS authentication.

The following are command flags of Pulsarctl. You can use `--auth-params` to specify the auth params configured in pulsar-client. Or you can just specify `--token` to use that token to connect the broker.

```
Common flags:
  -s, --admin-service-url string    The admin web service url that pulsarctl connects to. (default "http://localhost:8080")
      --auth-params string          Authentication parameters are used to configure the public and private key files required by tls
                                     For example: "tlsCertFile:val1,tlsKeyFile:val2"
  -C, --color string                toggle colorized logs (true,false,fabulous) (default "true")
  -h, --help                        help for this command
      --tls-allow-insecure          Allow TLS insecure connection
      --tls-trust-cert-path string   Allow TLS trust cert file path
      --token string                Using the token to authentication
      --token-file string           Using the token file to authentication
  -v, --verbose int                 set log level, use 0 to silence, 4 for debugging (default 3)
```

### BookKeeper operations

For BookKeeper operations, Pulsarctl integrates the commands listed in the [REST API](https://bookkeeper.apache.org/docs/4.10.0/admin/http/)

- auto-recovery operations
- bookie operations
- bookies operations
- ledger operations


### Save your configuration in the different contexts

Pulsarctl provides a context command which lets you manage your Pulsar cluster easier. The ‘context’ can save the different configurations of your Pulsar cluster. And you can easily change to another cluster by the command  `pulsarctl context use`. For more details please refer to https://github.com/streamnative/pulsarctl/blob/master/docs/en/how-to-use-context.md


### Get different formats of the output

Pulsarctl provides an output flag `--output` to make the output transform into different formats, such as text, JSON, and YAML. The default view is the text which allows you to check the resources in your cluster directly. Also, you can get the JSON or YAML format to do what you want to.


### Extend Pulsarctl using plugins easily

You can easily extend the Pulsarctl with a plugin. Pulsarctl will auto-find the installed plugins and you can use them directly in Pulsarctl. For more details about the plugin, please refer to https://github.com/streamnative/pulsarctl/blob/master/docs/en/how-to-extend-pulsarctl-with-plugins.md

### Auto-complete your command

Pulsarctl supports auto-completing your input command. If you forget some commands, try to `tab` it, Pulsarctl will remind you. For how to enable the auto-completion feature, please refer to:
https://github.com/streamnative/pulsarctl/blob/master/docs/en/enable_completion.md

### A Golang library of Pulsar admin

Pulsarctl is not only a command-line tool but also a library of Pulsar admin API. You can use it in your application to communicate with Pulsar. Here is an example of how to use Pulsarctl in your application: https://github.com/streamnative/pulsarctl/blob/master/docs/en/overview_of_pulsarctl.md#admin-api

## More information

- Pulsarctl project repo: https://github.com/streamnative/pulsarctl
- The latest Pulsarctl API website: https://streamnative.io/docs/pulsarctl/v0.4.1/
- Introduction to Pulsarctl: https://streamnative.io/blog/tech/2019-11-26-introduction-pulsarctl
