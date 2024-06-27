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


ARG GLIBC_VERSION=2.38
ARG ALPINE_VERSION=3.20

FROM ubuntu:22.04 as build
ARG GLIBC_VERSION

RUN apt-get -q update \
	&& apt-get -qy install \
		bison \
		build-essential \
		gawk \
		gettext \
		openssl \
		python3 \
		texinfo \
		wget

# Build GLibc
RUN wget -qO- https://ftpmirror.gnu.org/libc/glibc-${GLIBC_VERSION}.tar.gz | tar zxf -
RUN mkdir /glibc-build
WORKDIR /glibc-build
RUN /glibc-${GLIBC_VERSION}/configure \
        			--prefix=/usr/glibc-compat \
        			--libdir=/usr/glibc-compat/lib \
        			--libexecdir=/usr/glibc-compat/lib \
        			--enable-multi-arch \
        			--enable-stack-protector=strong
RUN make -j$(nproc)
RUN make install
RUN tar --dereference --hard-dereference -zcf /glibc-bin.tar.gz /usr/glibc-compat


################################################
## Build the APK package
FROM alpine:$ALPINE_VERSION as apk
ARG GLIBC_VERSION

RUN apk add abuild sudo build-base

RUN mkdir /build
WORKDIR build

COPY --from=build /glibc-bin.tar.gz /build

COPY ./scripts /build

RUN echo "pkgver=\"${GLIBC_VERSION}\"" >> /build/APKBUILD
RUN echo "sha512sums=\"$(sha512sum glibc-bin.tar.gz ld.so.conf)\"" >> /build/APKBUILD

RUN abuild-keygen -a -i -n
RUN abuild -F -c -r

################################################
## Last stage - Only leaves the packages
FROM busybox
ARG GLIBC_VERSION

RUN mkdir -p /root/packages
COPY --from=apk /root/packages/*/glibc-${GLIBC_VERSION}-r0.apk /root/packages
COPY --from=apk /root/packages/*/glibc-bin-${GLIBC_VERSION}-r0.apk /root/packages
