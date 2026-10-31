# Formal Bzlmod worker build, separate from worker.Dockerfile's tested CMake
# fallback. Client and every pool must deploy this SAME final image identity.
# CUDA 13.2 is the measured fallback; this does not claim planned CUDA 13.4.2.
ARG TOOLCHAIN_BASE=nvidia/cuda:13.2.0-devel-ubuntu24.04@sha256:d266e59b88c295bc5fa0e4cef9064eaff84939381b09e2c3d76a5532a303e42d
FROM ${TOOLCHAIN_BASE} AS toolchain
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl build-essential cmake ninja-build git python3 \
      pkg-config zip unzip patch file autoconf flex bison perl libfl-dev zstd \
      && rm -rf /var/lib/apt/lists/*
COPY scripts/bootstrap-bazel.sh /opt/remex/bootstrap/scripts/bootstrap-bazel.sh
RUN /opt/remex/bootstrap/scripts/bootstrap-bazel.sh \
    && cp /opt/remex/bootstrap/.tools/bazel.official /usr/local/bin/bazel \
    && rm -rf /opt/remex/bootstrap \
    && mkdir -p /usr/local/share/remex \
    && dpkg-query -W > /usr/local/share/remex/packages.tsv \
    && bazel --version > /usr/local/share/remex/bazel-version.txt \
    && nvcc --version > /usr/local/share/remex/cuda-version.txt

FROM toolchain AS worker-build
ARG BUILD_JOBS=16
ARG BUILD_MODE=opt
WORKDIR /opt/remex/src
COPY MODULE.bazel MODULE.bazel.lock ./
COPY worker ./worker
COPY api/worker/v1 ./api/worker/v1
# A BuildKit-owned cache retains source builds between image iterations; it is
# not a host filesystem mount. Only installed artifacts enter the final stage.
RUN --mount=type=cache,id=remex-bazel-ubuntu24-cuda132,target=/var/cache/remex-bazel \
    bazel --batch --output_user_root=/var/cache/remex-bazel build \
      --jobs="${BUILD_JOBS}" --compilation_mode="${BUILD_MODE}" --dynamic_mode=off \
      //worker:remex-worker //worker:cgroup-tests //worker:nvml-tests \
    && bazel --batch --output_user_root=/var/cache/remex-bazel test \
      --jobs="${BUILD_JOBS}" --compilation_mode="${BUILD_MODE}" --dynamic_mode=off --test_output=errors \
      //worker:worker-tests //worker:remote-tests //worker:polling-tests \
    && mkdir -p /opt/remex/artifacts \
    && install -m 0755 bazel-bin/worker/remex-worker /opt/remex/artifacts/remex-worker \
    && install -m 0755 bazel-bin/worker/cgroup-tests /opt/remex/artifacts/remex-cgroup-tests \
    && install -m 0755 bazel-bin/worker/nvml-tests /opt/remex/artifacts/remex-nvml-tests \
    && /opt/remex/artifacts/remex-worker --help >/dev/null \
    && ! ldd /opt/remex/artifacts/remex-cgroup-tests | grep -q 'not found' \
    && cp MODULE.bazel MODULE.bazel.lock /opt/remex/artifacts/ \
    && find worker api -type f -print0 | sort -z | xargs -0 sha256sum > /opt/remex/artifacts/worker-source.sha256 \
    && bazel --batch --output_user_root=/var/cache/remex-bazel query \
         'deps(//worker:remex-worker)' --output=label \
         > /opt/remex/artifacts/worker-dependencies.txt

FROM toolchain
COPY --from=worker-build /opt/remex/artifacts/remex-worker /usr/local/bin/remex-worker
COPY --from=worker-build /opt/remex/artifacts/remex-cgroup-tests /usr/local/bin/remex-cgroup-tests
COPY --from=worker-build /opt/remex/artifacts/remex-nvml-tests /usr/local/bin/remex-nvml-tests
COPY --from=worker-build /opt/remex/artifacts/MODULE.bazel /opt/remex/artifacts/MODULE.bazel.lock /opt/remex/artifacts/worker-dependencies.txt /opt/remex/artifacts/worker-source.sha256 /usr/local/share/remex/
ENV HOME=/tmp/remex-home
WORKDIR /workspace
USER 1000:1000
ENTRYPOINT ["remex-worker"]
CMD ["--help"]
