<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# The Pulsar website and documentation

This `README` is basically the meta-documentation for the Pulsar website and documentation. Here you'll find instructions on running the site locally

## Tools

This website is built using a wide range of tools. The most important among them:

* [Jekyll](https://jekyllrb.com/) is the static site generator
* CSS is compiled using [Sass](http://sass-lang.com/)
* [Swagger](https://swagger.io/) is used to create a JSON definition of Pulsar's admin REST API
* [Doxygen](http://www.stack.nl/~dimitri/doxygen/) is used to generate the C++ API docs
* [pdoc](https://github.com/BurntSushi/pdoc) is used to generate the Python API docs
* [Javadoc](http://www.oracle.com/technetwork/articles/java/index-jsp-135444.html) is used to generate the Java API docs
* [protoc-gen-doc](https://github.com/pseudomuto/protoc-gen-doc) is used to generate a JSON definition of Pulsar's [Protocol Buffers](https://developers.google.com/protocol-buffers/) interface (which is in turn used to auto-generate Protobuf docs)

## Requirements and setup

### MacOS

To build and run the site locally, you need to have Ruby 2.3.1 installed and set as the Ruby version here in the `site` directory. You can install and set the Ruby version using [rvm](https://rvm.io):

```bash
$ cd site
$ rvm install .
$ rvm use .
```

Then you can install all Ruby dependencies, as well as [Doxygen](http://www.stack.nl/~dimitri/doxygen/):

```bash
$ make setup
```

### Linux

**NOTE**: These instruction have been tested with Ubuntu 16.04 and Debian Stretch. YMMV with other distributions.

The site is built using Ruby 2.3.1. Neither Ubuntu nor Debian have this in their repositories, so it must be installed via RVM. Instructions for RVM installation are available [here](https://rvm.io/rvm/install). When RVM is installed, it will ask you to "source" a script. You can either add this to your .bashrc, or source it every time you build the the site. Once you have done so, install ruby-2.3.1.

```bash
$ source /usr/share/rvm/scripts/rvm
$ rvm install ruby-2.4.1
```

Doxygen, pdoc and pygments are required to build the C++ and python documentation. Pdoc and pygments are not in the Ubuntu/Debian repos. The best way to install them is in a virtualenv to avoid polluting your system packages.

```bash
$ sudo apt-get install doxygen python-virtualenv
$ virtualenv ~/pulsar-site-venv
$ source ~/pulsar-site-venv/bin/activate
(pulsar-site-venv) $ pip install pdoc pygments
```

Once all the dependencies are installed, change into the site directory and run setup to install all the required ruby gems.

```bash
(pulsar-site-venv) $ cd site
(pulsar-site-venv) $ rvm use .
(pulsar-site-venv) $ make setup
```

## Building the site

To build the site, run the build make target. If you are running in linux, you should do this on a terminal which has both the venv and rvm activated.

```bash
$ source ~/pulsar-site-venv/bin/activate # linux only
$ source /usr/share/rvm/scripts/rvm #linux only
$ cd site
$ make build
```

## Running the site locally

To run the site locally:

```bash
$ make serve
```

This will start up Jekyll in live mode. As you make changes to the site or to the Sass files, the site will auto-update (and the browser will auto-refresh). There is typically a time lag of roughly 1.5 seconds for the refresh.

## Building and publishing the site

Building and publishing the website is handled for you automatically at the CI level. No action is required on your part. If you push changes to the website to the `master` branch (via pull request), this will trigger a CI job that handles everything for you, *unless* you need to build one of the following locally:

* The Java, Python, or C++ API docs
* The Protobuf docs
* The REST API docs

If you'd like to view changes to those three things, you will need to build those parts of the site locally. Instructions for each are below.

### Java API docs

To re-generate the Java API docs, you'll need to have the `javadoc` CLI tool installed and on your `PATH` (`javadoc` comes as part of most Java distributions). Then you need to run:

```bash
$ make javadoc
```

### Python API docs

To re-generate the Python API docs, you'll need to have the `pdoc` and `pygments` libraries installed, which you can install by running:

```bash
$ make python_setup
```

To generate the Python docs:

```bash
$ make python_doc_gen
```

### C++ API docs

To generate C++ API docs, you'll first need to install Doxygen. To do so:

```bash
$ make doxygen_install
```

Then generate the docs:

```bash
$ make cpp_doc_gen
```

### Protobuf docs

To re-generate the Protobuf docs, you'll need to install the [protoc-gen-doc](https://github.com/pseudomuto/protoc-gen-doc) tool:

```bash
$ make protobuf_setup
```

To re-generate the Protobuf docs:

```bash
$ make protobuf_doc_gen
```

### REST API docs

The REST API docs are built using a JSON definition generated by Maven's Swagger plugin. This definition is automatically generated when Maven builds Pulsar. In the rare case when you do need to re-generate this JSON definition, run `mvn package -P swagger` to build all of Pulsar. Then you'll need to copy the new definition into the `site/_data` folder using this command:

```bash
$ make swagger_definition_copy
```

## Checking links

The [htmltest](https://github.com/wjdp/htmltest) tool is installed on MacOS when you run the `make setup` command. You can check links by running `make linkcheck_macos`. At the moment, linkchecking is performed ad hoc but may eventually be applied at the CI level.

## Contributing

We are very much open to your contributions! Pulsar is a big and ever-changing project, and it takes a lot of combined effort to keep the website looking good and the documentation up to date. Just fork the project, make changes locally, and submit a pull request to this repository.
