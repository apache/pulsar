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

set -exuo pipefail

PYTHON_VERSIONS=(
  '3.7  3.7.13 x86_64'
#  '3.7  3.7.13 arm64'
  '3.8  3.8.13 x86_64;arm64'
  '3.9  3.9.10 x86_64;arm64'
  '3.10 3.10.2 x86_64;arm64'
)

export MACOSX_DEPLOYMENT_TARGET=10.15

ZLIB_VERSION=1.2.12
OPENSSL_VERSION=1_1_1n
BOOST_VERSION=1.78.0
PROTOBUF_VERSION=3.20.0
ZSTD_VERSION=1.5.2
SNAPPY_VERSION=1.1.3
CURL_VERSION=7.61.0

ROOT_DIR=$(git rev-parse --show-toplevel)
cd "${ROOT_DIR}/pulsar-client-cpp"

# Compile and cache dependencies
CACHE_DIR=~/.pulsar-mac-wheels-cache
PYENV=$CACHE_DIR/pyenv/bin/pyenv
PREFIX=$CACHE_DIR/install
mkdir -p $PREFIX
cd $CACHE_DIR

if [ ! -x "$PYENV" ]; then
  git clone https://github.com/pyenv/pyenv.git pyenv
fi

if [ ! -d boost-src ]; then
  curl -O -L https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION//./_}.tar.gz
  mkdir boost-src
  tar xvfz boost_${BOOST_VERSION//./_}.tar.gz -C boost-src --strip-components=1
fi

export MAKEFLAGS=-j$(sysctl hw.physicalcpu | cut -d' ' -f2)
export SDKROOT=$(xcrun --show-sdk-path)
# Many MacOS systems have extensive customization of build-related variables. The below unsets those or sets them to
# minimal standard resource locations in hopes of increasing reproducibility so that this packaging can be run reliably
# from different Macs.
export PKG_CONFIG_PATH=
# OPENSSL_NO_SSL3: The presence of Homebrew-installed openssl@3 can cause things to accumulate unexpected linker
# dependencies, so ensure that it is not used.
export CFLAGS="-fPIC -O3 -DOPENSSL_NO_SSL3 -I${SDKROOT}/usr/include -I${PREFIX}/include -mmacosx-version-min=${MACOSX_DEPLOYMENT_TARGET}"
export CXXFLAGS="$CFLAGS"
export CPPFLAGS="$CFLAGS"
export LDFLAGS="-L${SDKROOT}/usr/lib -L${PREFIX}/lib"

###############################################################################
if [ ! -f openssl-OpenSSL_${OPENSSL_VERSION}.done ]; then
    echo "Building OpenSSL"
    curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_${OPENSSL_VERSION}.tar.gz
    tar xvfz OpenSSL_${OPENSSL_VERSION}.tar.gz
    mv openssl-OpenSSL_${OPENSSL_VERSION} openssl-OpenSSL_${OPENSSL_VERSION}-arm64
    pushd openssl-OpenSSL_${OPENSSL_VERSION}-arm64
      ./Configure --openssldir=$PREFIX --prefix=$PREFIX no-shared darwin64-arm64-cc
      make
      make install_sw  # Skip manpages
    popd

    tar xvfz OpenSSL_${OPENSSL_VERSION}.tar.gz
    mv openssl-OpenSSL_${OPENSSL_VERSION} openssl-OpenSSL_${OPENSSL_VERSION}-x86_64
    pushd openssl-OpenSSL_${OPENSSL_VERSION}-x86_64
      ./Configure --openssldir=$PREFIX --prefix=$PREFIX no-shared darwin64-x86_64-cc
      make
      make install_sw  # Skip manpages
    popd

    # Create universal binaries
    lipo -create openssl-OpenSSL_${OPENSSL_VERSION}-arm64/libssl.a openssl-OpenSSL_${OPENSSL_VERSION}-x86_64/libssl.a \
          -output $PREFIX/lib/libssl.a
    lipo -create openssl-OpenSSL_${OPENSSL_VERSION}-arm64/libcrypto.a openssl-OpenSSL_${OPENSSL_VERSION}-x86_64/libcrypto.a \
              -output $PREFIX/lib/libcrypto.a

    touch openssl-OpenSSL_${OPENSSL_VERSION}.done
else
    echo "Using cached OpenSSL"
fi


###############################################################################
if [ ! -f protobuf-${PROTOBUF_VERSION}/.done ]; then
    echo "Building Protobuf"
    curl -O -L  https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    tar xvfz protobuf-cpp-${PROTOBUF_VERSION}.tar.gz
    pushd protobuf-${PROTOBUF_VERSION}
      CXXFLAGS="-arch arm64 -arch x86_64 $CXXFLAGS" \
            ./configure --prefix=$PREFIX
      make
      make install
      touch .done
    popd
else
    echo "Using cached Protobuf"
fi

###############################################################################
if [ ! -f zstd-${ZSTD_VERSION}/.done ]; then
    echo "Building ZStd"
    curl -O -L https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz
    tar xvfz zstd-${ZSTD_VERSION}.tar.gz
    pushd zstd-${ZSTD_VERSION}
      CFLAGS="-arch arm64 -arch x86_64 $CFLAGS" \
        PREFIX=$PREFIX \
        make install
      touch .done
    popd
else
    echo "Using cached ZStd"
fi

###############################################################################
if [ ! -f snappy-${SNAPPY_VERSION}/.done ]; then
    echo "Building Snappy"
    curl -O -L https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz
    tar xvfz snappy-${SNAPPY_VERSION}.tar.gz
    pushd snappy-${SNAPPY_VERSION}
      CXXFLAGS="-arch arm64 -arch x86_64 $CXXFLAGS" \
            ./configure --prefix=$PREFIX
      make
      make install
      touch .done
    popd
else
    echo "Using cached Snappy"
fi

###############################################################################
if [ ! -f curl-${CURL_VERSION}/.done ]; then
    echo "Building LibCurl"
    CURL_VERSION_=${CURL_VERSION//./_}
    curl -O -L  https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_}/curl-${CURL_VERSION}.tar.gz
    tar xfz curl-${CURL_VERSION}.tar.gz
    pushd curl-${CURL_VERSION}
      CFLAGS="-arch arm64 -arch x86_64 $CFLAGS" \
            ./configure --with-ssl=$PREFIX \
              --without-nghttp2 --without-libidn2 --disable-ldap \
              --prefix=$PREFIX
      make install
      touch .done
    popd
else
    echo "Using cached LibCurl"
fi

###############################################################################
# For each python/arch tuple, build Python at that version via Pyenv, then install pip/setuptools/wheel in that pyenv,
# then install boost-python linked to the pyenv's python, then build the pulsar client in that directory.
for line in "${PYTHON_VERSIONS[@]}"; do
    read -r -a PY <<< "$line"
    PYTHON_VERSION=${PY[0]}
    PYTHON_VERSION_LONG=${PY[1]}
    ARCHS=${PY[2]}

    echo "Building Python $PYTHON_VERSION_LONG for $ARCHS"
    ARCHCMD=""
    if [ "$ARCHS" = "x86_64;arm64" ]; then
      # PYENV_ROOT is exported rather than set since it is a variable used by pyenv itself to determine where it should
      # place its built python versions. Since we are building Python with nonstandard flags, we set PYENV_ROOT to
      # pulsar-wheel-build-specific locations so we neither pollute nor accidentally use pyenv-managed Pythons provided
      # by system-level pyenv. Without this, for example, a user who did `brew install pyenv; pyenv install 3.8.13`
      # would cause the pulsar client build to use a non-universal-architecture-supporting Python 3.8.
      export PYENV_ROOT=$CACHE_DIR/python-$PYTHON_VERSION_LONG-universal
      PYTHON_CONFIGURE_OPTS="--enable-universalsdk --with-universal-archs=universal2"
    else
      export PYENV_ROOT=$CACHE_DIR/Python-$PYTHON_VERSION_LONG-$ARCHS
      PYTHON_CONFIGURE_OPTS=""
      # Check if we need to use rosetta; this will cause failures on Intel macs as they can't crossbuild for ARM; it
      # only goes the other direction.
      if [ "$(arch)" != $ARCHS ]; then
        ARCHCMD="arch -$ARCHS"
      fi
    fi
    PY_PREFIX=$PYENV_ROOT/versions/$PYTHON_VERSION_LONG
    PY_INCLUDE_DIR=${PY_PREFIX}/include/python${PYTHON_VERSION}
    if [ $PYTHON_VERSION = '3.7' ]; then
      PY_INCLUDE_DIR=${PY_INCLUDE_DIR}m
    fi

    PYTHON_CONFIGURE_OPTS="--without-readline --with-openssl=$PREFIX --enable-shared $PYTHON_CONFIGURE_OPTS" \
      $ARCHCMD $PYENV install -s $PYTHON_VERSION_LONG

    pushd $PYENV_ROOT
    $PYENV local $PYTHON_VERSION_LONG
    $PYENV exec python -mpip install --upgrade pip
    $PYENV exec python -mpip install --upgrade setuptools wheel

    DIR=boost-src-${BOOST_VERSION}-python-${PYTHON_VERSION}

    if [ ! -f $DIR/.done ]; then
        echo "Building Boost for Py $PYTHON_VERSION at $ARCHS"
        rm -rf $DIR
        cp -r "$CACHE_DIR/boost-src" $DIR
        pushd $DIR
        cat <<EOF > user-config.jam
            using python : $PYTHON_VERSION
                    : python3
                    : ${PY_INCLUDE_DIR}
                    : ${PY_PREFIX}/lib
                  ;
EOF
        ./bootstrap.sh --with-libraries=python --with-python=python3 --with-python-root=$PY_PREFIX \
              --prefix=$CACHE_DIR/boost-py-$PYTHON_VERSION
        ./b2 address-model=64 cxxflags="-arch arm64 -arch x86_64 $CFLAGS" \
                  link=static threading=multi \
                  --user-config=./user-config.jam \
                  variant=release python=${PYTHON_VERSION} \
                  $MAKEFLAGS \
                  install
        touch .done
        popd
    else
        echo "Using cached Boost for Py $PYTHON_VERSION at $ARCHS"
    fi
    popd

    echo "Building pulsar-client-cpp for python $PYTHON_VERSION at $ARCHS"
    pushd "${ROOT_DIR}/pulsar-client-cpp"
    $PYENV local $PYTHON_VERSION_LONG
    find . -name CMakeCache.txt | xargs -r rm
    find . -name CMakeFiles | xargs -r rm -rf
    cmake . \
            -DCMAKE_OSX_ARCHITECTURES=${ARCHS} \
            -DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET} \
            -DCMAKE_INSTALL_PREFIX=$PREFIX \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_PREFIX_PATH=$PREFIX \
            -DCMAKE_CXX_FLAGS=-I$PREFIX/include \
            -DBoost_INCLUDE_DIR=$CACHE_DIR/boost-py-$PYTHON_VERSION/include \
            -DBoost_LIBRARY_DIR=$CACHE_DIR/boost-py-$PYTHON_VERSION/lib \
            -DPYTHON_INCLUDE_DIR=$PY_INCLUDE_DIR \
            -DPYTHON_LIBRARY=$PY_PREFIX/lib/libpython${PYTHON_VERSION}.dylib \
            -DLINK_STATIC=ON \
            -DBUILD_TESTS=OFF \
            -DBUILD_WIRESHARK=OFF \
            -DPROTOC_PATH=$PREFIX/bin/protoc

    make clean
    make _pulsar

    echo "Building wheel for python $PYTHON_VERSION at $ARCHS"
    pushd python
    $PYENV exec python setup.py bdist_wheel
    popd
    popd
done
