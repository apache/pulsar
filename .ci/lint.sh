set -e

BINDIR=`dirname "$0"`
CI_HOME=`cd ${BINDIR};pwd`

${CI_HOME}/ct.sh -c lint
