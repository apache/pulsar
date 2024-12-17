ARG IMAGE_JDK_MAJOR_VERSION=21

# Build stage
FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine AS kpl-build

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
RUN chmod +x /build/build-alpine.sh && \
    cd /build && \
    ./build-alpine.sh

# Runtime stage
FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine

# Install runtime dependencies
RUN apk add --no-cache \
    libuuid \
    libstdc++ \
    zlib \
    boost \
    openssl \
    curl

# Copy built artifacts
COPY --from=kpl-build /build/amazon-kinesis-producer/java/amazon-kinesis-producer/target/amazon-kinesis-producer-*.jar /opt/kpl/
COPY --from=kpl-build /build/amazon-kinesis-producer/java/amazon-kinesis-producer/src/main/resources/amazon-kinesis-producer-native-binaries/ /opt/kpl/native-binaries/

# Set environment variables
ENV LD_LIBRARY_PATH=/opt/kpl/native-binaries/linux-x86_64
ENV JAVA_HOME=/usr/lib/jvm/java-${IMAGE_JDK_MAJOR_VERSION}-amazon-corretto

# Set working directory
WORKDIR /opt/kpl

# Command to verify installation (can be modified based on your needs)
CMD ["java", "-version"]