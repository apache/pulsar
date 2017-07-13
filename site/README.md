# The Pulsar website and documentation

## Requirements and setup

> **Note**: at the moment, some aspects of the site, such as the C++ docs, can only be built on MacOS. This will change in the near future.

To run a basic version of the site, you need to have Ruby 2.4.1 installed and set as the Ruby version here in the `site` directory. You can install and set the Ruby version using [rvm](https://rvm.io):

```bash
$ cd site
$ rvm install .
$ rvm use .
```

Then you can install all Ruby dependencies, as well as [Doxygen](http://www.stack.nl/~dimitri/doxygen/):

```bash
$ make setup
```

## Building the site

```bash
$ make 
```

## Running the site locally