---
id: reference-cli-pulsar-shell
title: Pulsar CLI tools - pulsar-shell
sidebar_label: "cli-pulsar-shell"
---

## `pulsar-shell`

[Pulsar shell](administration-pulsar-shell.md) tool.

### Interactive mode

Usage

```bash
pulsar-shell
```

Options

| Flag               | Description                                                               | Default          |
|--------------------|---------------------------------------------------------------------------|------------------|
| `-c`, `--config`   | Client configuration file. It is used as a `default` config.           | conf/client.conf | 
| `--fail-on-error` | If true, the shell is interrupted when a command throws an exception.  | false            | 
| `-h`, `--help`     | Show this help.                                                            | |


### Non interactive mode

Usage

```bash
pulsar-shell -f [FILE]
pulsar-shell -e [COMMAND]
echo "[COMMAND]" | pulsar-shell -
```

Options

| Flag                      | Description                                                                                         | Default         |
|---------------------------|-----------------------------------------------------------------------------------------------------|-----------------|
| `-c`, `--config`          | Client configuration file. It is used as a `default` config.                                     | conf/client.conf | 
| `--fail-on-error`         | If true, the shell is interrupted when a command throws an exception.                            | false           | 
| `-np`, `--no-progress`    | Display raw output of the commands without the fancy progress visualization.                        | false           | 
| `-f`, `--filename`        | Input filename with a list of commands to be executed. Each command must be separated by a newline. |                 |
| `-e`, `--execute-command` | Execute this command and exit.                                                                      | |
| `-` | Read commands from the standard input.                                                              | |
| `-h`, `--help`     | Show this help.                                                                                      | |


Commands
* `admin` - See [Admin API](admin-api-overview.md)
* `client` - See [pulsar-client](#pulsar-client)
* `config`


### `config`

Manage shell configurations.

#### `use`

Use a specific configuration for next commands.

```bash
default(localhost)> config use mycluster
```

#### `create`

Create a new configuration.

```bash
default(localhost)> config create --file ./conf/client.conf mycluster
```

Options

| Flag     | Description              | Default         |
|----------|--------------------------|-----------------|
| `--file` | File path of the config. |  | 
| `--url`  | URL of the config.       |  |
| `--value`  | Inline value of the config. Base64-encoded value is supported with the prefix `base64:`. |  |

#### `update`

Update an existing configuration.

```bash
default(localhost)> config update --file ./conf/client.conf mycluster
```

Options

| Flag     | Description              | Default         |
|----------|--------------------------|-----------------|
| `--file` | File path of the config. |  | 
| `--url`  | URL of the config.       |  |
| `--value`  | Inline value of the config. Base64-encoded value is supported with the prefix `base64:`. |  |

#### `set-property`

Set a value for a specified configuration property.

```bash
default(localhost)> config set-property -p webServiceUrl -v http://<cluster-hostname> mycluster
```

Options

| Flag               | Description                 | Default         |
|--------------------|-----------------------------|-----------------|
| `-p`, `--property` | Property name to update.    |  | 
| `-v`, `--value`    | New value for the property. |  |


#### `get-property`

Get the value for a specified configuration property.

```bash
default(localhost)> config get-property -p webServiceUrl mycluster
```

Options

| Flag               | Description                 | Default         |
|--------------------|-----------------------------|-----------------|
| `-p`, `--property` | Property name to update.    |  | 


#### `view`

View details of a config.

```bash
default(localhost)> config view mycluster
```

#### `delete`

Delete a config. You can't delete a config if it's currently used.

```bash
default(localhost)> config delete mycluster
```


#### `list`

List all the configuration names.

```bash
default(localhost)> config list
```