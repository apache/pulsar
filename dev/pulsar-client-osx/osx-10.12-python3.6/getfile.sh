#!/bin/bash

set -e

gitTag="${gitTag:-master}"
pythonVer="PYTHON3"

rm -rf *.whl
sed s/#TAG#/$gitTag/g build.sh.template > build.sh.template2
sed s/#PYTHONVER#/$pythonVer/g build.sh.template2 > build.sh
vagrant up --provision
vagrant scp :/Users/vagrant/incubator-pulsar/pulsar-client-cpp/python/dist/*.whl .
vagrant halt -f