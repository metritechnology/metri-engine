FROM clojure:tools-deps
RUN apt-get update && apt-get install -y protobuf-compiler wget
RUN wget -qO /usr/local/bin/protoc-gen-grpc-java https://repo1.maven.org/maven2/io/grpc/protoc-gen-grpc-java/1.62.2/protoc-gen-grpc-java-1.62.2-linux-aarch_64.exe && chmod +x /usr/local/bin/protoc-gen-grpc-java
ENV PROTOC_BIN=protoc
ENV PROTOC_INC=/usr/include
ENV PROTOC_PLUGIN=/usr/local/bin/protoc-gen-grpc-java
WORKDIR /app
CMD ["clj", "-T:build", "uber"]
