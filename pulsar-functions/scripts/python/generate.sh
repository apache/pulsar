#!/usr/bin/env bash
TMP_DIR=$(mktemp -d -t pulsarfunction.XXXXX)
PF_DIR=$TMP_DIR/pulsarfunction
OUTPUT_DIR=$2
VERSION=$3
echo $VERSION
mkdir -p $TMP_DIR $PF_DIR $PF_DIR/pulsarfunction
cp $1/*.py $PF_DIR/pulsarfunction
echo "__import__(\'pkg_resources\').declare_namespace(__name__)" > $PF_DIR/pulsarfunction/__init__.py
sed "s/VERSION/$VERSION/" setup.py.template > $PF_DIR/setup.py
cp requirements.txt $PF_DIR
cd $PF_DIR
/usr/bin/env python2.7 setup.py sdist
/usr/bin/env python2.7 setup.py bdist_wheel
mkdir -p $OUTPUT_DIR
cp $PF_DIR/dist/pulsarfunction-*-py2-*.whl $OUTPUT_DIR
cp $PF_DIR/dist/pulsarfunction-*.tar.gz $OUTPUT_DIR
touch $OUTPUT_DIR/pulsarfunction.whl
rm -rf $TMP_DIR


