# syntax=docker/dockerfile:1-labs

FROM cgr.dev/chainguard/rust:latest-dev AS builder
WORKDIR /src
ENV RUST_BACKTRACE=1

# tools
USER root
RUN apk --no-cache add protoc protobuf protobuf-dev
USER nonroot

# copy source code
COPY . .

# build with cached dependencies and compilation artifacts
RUN --mount=type=cache,target=/home/nonroot/.cargo/registry,uid=65532 \
    --mount=type=cache,target=/home/nonroot/.cargo/git,uid=65532 \
    --mount=type=cache,target=/src/target,uid=65532 \
    cargo build --release --bin wash && \
    cp target/release/wash /tmp/wash

# Release image
FROM cgr.dev/chainguard/wolfi-base
RUN apk add --no-cache git
COPY --from=builder /tmp/wash /usr/local/bin/wash
ENTRYPOINT ["/usr/local/bin/wash"]
