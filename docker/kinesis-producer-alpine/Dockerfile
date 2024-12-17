ARG IMAGE_JDK_MAJOR_VERSION=21

# Build stage
FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine

# Install build dependencies
RUN apk update && apk add --no-cache \
    git \
    binutils \
    coreutils \
    alpine-sdk \
    util-linux \
    cmake \
    autoconf \
    automake \
    libtool \
    maven \
    curl \
    bash \
    tar \
    libuuid \
    linux-headers \
    zlib \
    zlib-dev \
    perl \
    wget \
    boost-dev \
    openssl-dev \
    curl-dev \
    build-base \
    util-linux-dev \
    g++ \
    make

ENV LANG=C.UTF-8
ENV KINESIS_PRODUCER_LIB_VERSION=0.15.12

# Clone KPL and copy build script
RUN mkdir /build && \
    cd /build && \
    git clone --depth 1 --single-branch --branch v${KINESIS_PRODUCER_LIB_VERSION} https://github.com/awslabs/amazon-kinesis-producer

# Copy and execute build script
COPY build-alpine.sh /build/
RUN chmod +x /build/build-alpine.sh
WORKDIR /build