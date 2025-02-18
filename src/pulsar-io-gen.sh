#!/usr/bin/env bash
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

BINDIR=$(dirname "$0")
PULSAR_HOME=`cd $BINDIR/..;pwd`

DEFAULT_LOG_CONF=$PULSAR_HOME/conf/log4j2.yaml

# Check bookkeeper env and load bkenv.sh
if [ -f "$PULSAR_HOME/conf/bkenv.sh" ]
then
    . "$PULSAR_HOME/conf/bkenv.sh"
fi

if [ -f "$PULSAR_HOME/conf/pulsar_env.sh" ]
then
    . "$PULSAR_HOME/conf/pulsar_env.sh"
fi

# Check for the java to use
if [[ -z $JAVA_HOME ]]; then
    JAVA=$(which java)
    if [ $? != 0 ]; then
        echo "Error: JAVA_HOME not set, and no java executable found in $PATH." 1>&2
        exit 1
    fi
else
    JAVA=$JAVA_HOME/bin/java
fi

# exclude tests jar
RELEASE_JAR=`ls $PULSAR_HOME/pulsar-*.jar 2> /dev/null | grep -v tests | tail -1`
if [ $? == 0 ]; then
    PULSAR_JAR=$RELEASE_JAR
fi

# exclude tests jar
BUILT_JAR=`ls $PULSAR_HOME/pulsar-io/docs/target/pulsar-*.jar 2> /dev/null | grep -v tests | tail -1`
if [ $? != 0 ] && [ ! -e "$PULSAR_JAR" ]; then
    echo "\nCouldn't find pulsar jar.";
    echo "Make sure you've run 'mvn package'\n";
    exit 1;
elif [ -e "$BUILT_JAR" ]; then
    PULSAR_JAR=$BUILT_JAR
fi

add_maven_deps_to_classpath() {
    MVN="mvn"
    if [ "$MAVEN_HOME" != "" ]; then
	MVN=${MAVEN_HOME}/bin/mvn
    fi

    # Need to generate classpath from maven pom. This is costly so generate it
    # and cache it. Save the file into our target dir so a mvn clean will get
    # clean it up and force us create a new one.
    f="${PULSAR_HOME}/pulsar-io/docs/target/classpath.txt"
    if [ ! -f "${f}" ]
    then
	${MVN} -f "${PULSAR_HOME}/pulsar-io/docs/pom.xml" dependency:build-classpath -Dmdep.outputFile="${f}" &> /dev/null
    fi
    PULSAR_CLASSPATH=${CLASSPATH}:`cat "${f}"`
}
pulsar_help() {
    cat <<EOF
Usage: pulsar <command>
where command is one of:
    conf                    Generate sample yaml files

    help                           This help message

or command is the full name of a class with a defined main() method.

Environment variables:
   PULSAR_LOG_CONF               Log4j configuration file (default $DEFAULT_LOG_CONF)
   PULSAR_EXTRA_OPTS             Extra options to be passed to the jvm
   PULSAR_EXTRA_CLASSPATH        Add extra paths to the pulsar classpath

These variable can also be set in conf/pulsar_env.sh
EOF
}

if [ -d "$PULSAR_HOME/lib" ]; then
	PULSAR_CLASSPATH="$PULSAR_CLASSPATH:$PULSAR_HOME/lib/*"
else
    add_maven_deps_to_classpath
fi

if [ -z "$PULSAR_LOG_CONF" ]; then
    PULSAR_LOG_CONF=$DEFAULT_LOG_CONF
fi

PULSAR_CLASSPATH="$PULSAR_JAR:$PULSAR_HOME/pulsar-io/docs/target/pulsar-io-docs.jar:$PULSAR_CLASSPATH:$PULSAR_EXTRA_CLASSPATH"
PULSAR_CLASSPATH="`dirname $PULSAR_LOG_CONF`:$PULSAR_CLASSPATH"
OPTS="-Djava.net.preferIPv4Stack=true $OPTS -Dlog4j.configurationFile=`basename $PULSAR_LOG_CONF`"

OPTS="-cp $PULSAR_CLASSPATH $OPTS"
OPTS="$OPTS $PULSAR_EXTRA_OPTS"

# log directory & file
PULSAR_LOG_APPENDER=${PULSAR_LOG_APPENDER:-"Console"}
PULSAR_LOG_DIR=${PULSAR_LOG_DIR:-"$PULSAR_HOME/logs"}
PULSAR_LOG_FILE=${PULSAR_LOG_FILE:-"pulsar-perftest.log"}

#Configure log configuration system properties
OPTS="$OPTS -Dpulsar.log.appender=$PULSAR_LOG_APPENDER"
OPTS="$OPTS -Dpulsar.log.dir=$PULSAR_LOG_DIR"
OPTS="$OPTS -Dpulsar.log.file=$PULSAR_LOG_FILE"

#Change to PULSAR_HOME to support relative paths
cd "$PULSAR_HOME"

# if no args specified, show usage
if [ $# = 0 ]; then
    pulsar_help;
    exit 1;
fi

# get arguments
COMMAND=$1
shift

if [ "$COMMAND" == "conf" ]; then
    echo $JAVA $OPTS org.apache.pulsar.io.docs.ConnectorDocGenerator "$@"
    exec $JAVA $OPTS org.apache.pulsar.io.docs.ConnectorDocGenerator "$@"
else
    pulsar_help;
fi
