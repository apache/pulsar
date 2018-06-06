#!/bin/bash

set -e

printf "== Generate python 2.7 osx packages ==\n"

cd osx-10.11-python2.7
sh generate-wheel-file.sh
cd ..

cd osx-10.12-python2.7
sh generate-wheel-file.sh
cd ..

cd osx-10.13-python2.7
sh generate-wheel-file.sh
cd ..

printf "== Generate python 3.6 osx packages ==\n"

cd osx-10.11-python3.6
sh generate-wheel-file.sh
cd ..

cd osx-10.12-python3.6
sh generate-wheel-file.sh
cd ..

cd osx-10.13-python3.6
sh generate-wheel-file.sh
cd ..