#!/bin/bash
set -e

ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ]; then
  PROTOC_ARCH="aarch_64"
else
  PROTOC_ARCH="x86_64"
fi

apt-get update && apt-get install -y wget unzip

mkdir -p .cache

if [ ! -f ".cache/protoc-29.3-linux-${PROTOC_ARCH}.zip" ]; then
  wget -q "https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-${PROTOC_ARCH}.zip" -O ".cache/protoc-29.3-linux-${PROTOC_ARCH}.zip"
fi

if [ ! -f ".cache/protoc-gen-grpc-java-1.73.0-linux-${PROTOC_ARCH}.exe" ]; then
  wget -q "https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.73.0/protoc-gen-grpc-java-1.73.0-linux-${PROTOC_ARCH}.exe" -O ".cache/protoc-gen-grpc-java-1.73.0-linux-${PROTOC_ARCH}.exe"
fi

if [ ! -d "/tmp/protoc" ]; then
  unzip -q ".cache/protoc-29.3-linux-${PROTOC_ARCH}.zip" -d /tmp/protoc
  cp ".cache/protoc-gen-grpc-java-1.73.0-linux-${PROTOC_ARCH}.exe" /tmp/protoc/bin/protoc-gen-grpc-java
  chmod +x /tmp/protoc/bin/protoc-gen-grpc-java
fi

export PROTOC_BIN=/tmp/protoc/bin/protoc
export PROTOC_INC=/tmp/protoc/include
export PROTOC_PLUGIN=/tmp/protoc/bin/protoc-gen-grpc-java

clj -T:build uber
