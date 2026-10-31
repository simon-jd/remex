# One image is used for both Bazel clients and all worker pools. For CUDA,
# override TOOLCHAIN_BASE with a tested, digest-pinned CUDA devel Ubuntu image.
ARG TOOLCHAIN_BASE=ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55
FROM ${TOOLCHAIN_BASE}
ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_JOBS=4
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl build-essential cmake ninja-build git python3 \
      pkg-config zip unzip patch file libprotobuf-dev protobuf-compiler \
      libgrpc++-dev protobuf-compiler-grpc libssl-dev libabsl-dev \
      verilator zstd && rm -rf /var/lib/apt/lists/*
# This CMake build deliberately uses Ubuntu's gRPC/protobuf packages. It does
# not claim verification of the separate Bazel-pinned gRPC 1.84.0 build.
WORKDIR /opt/remex/src
COPY worker ./worker
COPY api/worker/v1/worker.proto ./api/worker/v1/worker.proto
COPY scripts/bootstrap-bazel.sh ./scripts/bootstrap-bazel.sh
RUN ./scripts/bootstrap-bazel.sh && cp .tools/bazel.official /usr/local/bin/bazel \
    && cmake -S worker -B /opt/remex/build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /opt/remex/build --parallel "${BUILD_JOBS}" \
    && ctest --test-dir /opt/remex/build --output-on-failure -E worker-cgroup \
    && install -m 0755 /opt/remex/build/remex-worker /usr/local/bin/remex-worker \
    && install -m 0755 /opt/remex/build/cgroup-tests /usr/local/bin/remex-cgroup-tests \
    && mkdir -p /usr/local/share/remex \
    && dpkg-query -W > /usr/local/share/remex/packages.tsv \
    && bazel --version > /usr/local/share/remex/bazel-version.txt \
    && rm -rf /opt/remex/src /opt/remex/build
ENV HOME=/tmp/remex-home
WORKDIR /workspace
USER 1000:1000
ENTRYPOINT ["remex-worker"]
CMD ["--help"]
