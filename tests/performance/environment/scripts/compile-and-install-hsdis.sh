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

# Builds hsdis, the disassembler plugin that -XX:+PrintAssembly and JMH's perfasm profiler use, for the Amazon
# Corretto JDK that `java` runs, and installs it into that JDK's lib/server directory.
#
# - reads the Corretto version from `java -version`, such as 25.0.4.8.1, and checks out that tag of
#   https://github.com/corretto/corretto-<major>, so that hsdis matches the JVM
# - builds hsdis against the binutils sources (GPL licensed, which is why JDKs don't ship hsdis), with the
#   JDK itself as the boot JDK
# - checks that the JVM loads it
#
# It assumes a Corretto JDK installed with SDKMAN!, such as with `sdk i java 25.0.4-amzn` (`sdk l java` lists the
# JDKs; the Corretto ones end in -amzn), which is in the user's home directory, so that hsdis installs into it without
# root. It needs JDK 19 or later, whose build has the build-hsdis target, and about 2 GB of space for the build.
# Settings:
#   BINUTILS_VERSION    the binutils release to build against (default 2.43)
#   HSDIS_BUILD_DIR     where the sources are checked out and built, and kept for another build; by default a new
#                       directory under $TMPDIR (or /tmp), which is removed after a successful build and kept after a
#                       failed one
#   HSDIS_INSTALL_DIR   where hsdis is installed (default <java.home>/lib/server)
set -euo pipefail

BINUTILS_VERSION="${BINUTILS_VERSION:-2.43}"

fail() {
    echo "Error: $*" >&2
    exit 1
}

usage() {
    echo "Usage: $0" >&2
    echo "  Builds hsdis for the Corretto JDK that 'java' runs and installs it into that JDK." >&2
}

for arg in "$@"; do
    case "${arg}" in
        -h | --help)
            usage
            exit 0
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

command -v java > /dev/null || fail "java isn't on the PATH"

# The runtime line, such as "OpenJDK Runtime Environment Corretto-25.0.4.8.1 (build 25.0.4.1+8-LTS)"
version_output="$(java -version 2>&1)"
if [[ ! "${version_output}" =~ Runtime\ Environment\ Corretto-([0-9]+(\.[0-9]+)+) ]]; then
    echo "${version_output}" >&2
    fail "java isn't Amazon Corretto: 'java -version' has no 'Runtime Environment Corretto-<version>'"
fi
corretto_version="${BASH_REMATCH[1]}"
major_version="${corretto_version%%.*}"
if ((major_version < 19)); then
    fail "Corretto ${corretto_version} is too old: the JDK build has the build-hsdis target from JDK 19 on"
fi

java_home="$(java -XshowSettings:properties -version 2>&1 | sed -n 's/^ *java\.home = //p')"
[[ -n "${java_home}" && -d "${java_home}" ]] || fail "couldn't find java.home of $(command -v java)"

case "$(uname -m)" in
    x86_64) hsdis_arch=amd64 ;;
    aarch64 | arm64) hsdis_arch=aarch64 ;;
    *) fail "unsupported architecture $(uname -m)" ;;
esac
hsdis_library="hsdis-${hsdis_arch}.so"
install_dir="${HSDIS_INSTALL_DIR:-${java_home}/lib/server}"

missing=()
for tool in gcc g++ make autoconf zip unzip curl git tar; do
    command -v "${tool}" > /dev/null || missing+=("${tool}")
done
# binutils links the system's zlib
if ! echo '#include <zlib.h>' | gcc -E - > /dev/null 2>&1; then
    missing+=("zlib.h")
fi
if ((${#missing[@]} > 0)); then
    fail "missing ${missing[*]}; on Debian or Ubuntu:" \
        "sudo apt install build-essential autoconf zip unzip curl git zlib1g-dev"
fi

if [[ -n "${HSDIS_BUILD_DIR:-}" ]]; then
    build_dir="${HSDIS_BUILD_DIR}"
    mkdir -p "${build_dir}"
else
    build_dir="$(mktemp -d "${TMPDIR:-/tmp}/hsdis-build.XXXXXX")"
    # Removed when the build succeeds; kept when it fails, for its logs
    remove_build_dir() {
        if [[ $? -eq 0 ]]; then
            rm -rf "${build_dir}"
        else
            echo "The build directory ${build_dir} is kept for diagnosis; remove it when done" >&2
        fi
    }
    trap remove_build_dir EXIT
fi

echo "Building hsdis for Corretto ${corretto_version} (${java_home}) with binutils ${BINUTILS_VERSION} in ${build_dir}"

source_dir="${build_dir}/corretto-${corretto_version}"
if [[ ! -d "${source_dir}/.git" ]]; then
    git clone --depth 1 --branch "${corretto_version}" \
        "https://github.com/corretto/corretto-${major_version}.git" "${source_dir}"
fi

binutils_dir="${source_dir}/binutils-${BINUTILS_VERSION}"
if [[ ! -d "${binutils_dir}" ]]; then
    curl -fsSL "https://sourceware.org/pub/binutils/releases/binutils-${BINUTILS_VERSION}.tar.gz" \
        | tar -xz -C "${source_dir}"
fi

cd "${source_dir}"
if ! compgen -G "build/*/spec.gmk" > /dev/null; then
    # configure requires the headers of CUPS, fontconfig and ALSA even for a headless JDK, for libawt, which the
    # build-hsdis target doesn't build: stub headers satisfy its checks, so they needn't be installed
    stub_dir="${build_dir}/stubs"
    mkdir -p "${stub_dir}/cups" "${stub_dir}/fontconfig"
    for header in cups/cups.h cups/ppd.h fontconfig/fontconfig.h; do
        echo "/* A stub for configure: the build-hsdis target doesn't use it */" > "${stub_dir}/${header}"
    done
    # configure builds binutils, whose documentation needs makeinfo from texinfo 4.7 or later: a stub that reports
    # such a version and writes nothing skips the documentation
    cat > "${stub_dir}/makeinfo" << 'EOF_MAKEINFO'
#!/bin/sh
if [ "$1" = "--version" ]; then
    echo "makeinfo (GNU texinfo) 7.1"
fi
exit 0
EOF_MAKEINFO
    chmod +x "${stub_dir}/makeinfo"
    MAKEINFO="${stub_dir}/makeinfo" bash configure \
        --with-boot-jdk="${java_home}" \
        --enable-headless-only \
        --with-hsdis=binutils \
        --with-binutils-src="${binutils_dir}" \
        --with-cups-include="${stub_dir}" \
        --with-fontconfig-include="${stub_dir}" \
        --with-alsa-include="${stub_dir}" \
        --with-alsa-lib="${stub_dir}" \
        --with-freetype=bundled \
        --with-zlib=bundled
fi
make build-hsdis

built_library="$(find build -path "*/support/hsdis/${hsdis_library}" -print -quit)"
[[ -n "${built_library}" ]] || fail "the build left no ${hsdis_library} under ${source_dir}/build"

if [[ ! -w "${install_dir}" ]]; then
    fail "${install_dir} isn't writable; install ${source_dir}/${built_library} there as its owner," \
        "or set HSDIS_INSTALL_DIR"
fi
install -m 644 "${built_library}" "${install_dir}/${hsdis_library}"
echo "Installed ${install_dir}/${hsdis_library}"

# The JVM warns when it can't load hsdis
if java -XX:+UnlockDiagnosticVMOptions -XX:+PrintAssembly -version 2>&1 | grep -q "Loading hsdis library failed"; then
    fail "the JVM didn't load ${install_dir}/${hsdis_library}"
fi
echo "The JVM loads hsdis: -XX:+PrintAssembly and JMH's -prof perfasm can disassemble the JIT's code"
