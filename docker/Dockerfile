# syntax=docker/dockerfile:1.16.0
# check=error=true

# Multi-stage build for the PostgreSQL-Trino Gateway.
#
# Builder stage compiles a release binary; final stage is UBI 10 minimal
# with a non-root stackable user, the binary in /usr/local/bin, and no
# toolchain or build tooling carried into the runtime image.
#
# Stackable internal CI may override BUILDER_IMAGE with the SDP builder
# (oci.stackable.tech/sdp/ubi9-rust-builder); the COPY+cargo step below
# is a no-op when the binary is already present at /app.

ARG BUILDER_IMAGE=docker.io/library/rust:1.93-slim-bookworm

# hadolint ignore=DL3006
FROM ${BUILDER_IMAGE} AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY tests ./tests

RUN if [ ! -f /app/postgresql-trino-gateway ]; then \
        cargo build --release --locked && \
        cp target/release/postgresql-trino-gateway /app/postgresql-trino-gateway; \
    fi

# Find the latest version at https://catalog.redhat.com/software/containers/ubi10/ubi-minimal
# IMPORTANT: Use the "Manifest List Digest" (multi-arch) rather than the
# single-arch "Image Digest".
FROM registry.access.redhat.com/ubi10/ubi-minimal AS final

ARG VERSION
ARG RELEASE="1"
ARG STACKABLE_USER_GID="574654813"
ARG STACKABLE_USER_UID="782252253"
ARG STACKABLE_USER_NAME="stackable"

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

LABEL name="PostgreSQL-Trino Gateway"
LABEL maintainer="info@stackable.tech"
LABEL vendor="Stackable GmbH"
LABEL version="${VERSION}"
LABEL release="${RELEASE}"
LABEL summary="PostgreSQL wire-protocol shim in front of Trino."
LABEL description="A PostgreSQL wire-protocol gateway that translates queries and forwards them to Trino's REST API."

LABEL vcs-ref=""
LABEL distribution-scope="public"
LABEL url="https://stackable.tech"
ARG TARGETARCH
LABEL architecture="${TARGETARCH}"
LABEL com.redhat.component=""
# hadolint ignore=DL3048
LABEL com.redhat.license_terms=""
LABEL io.buildah.version=""
LABEL io.openshift.expose-services="5432:postgres"

LABEL org.opencontainers.image.authors="info@stackable.tech"
LABEL org.opencontainers.image.url="https://stackable.tech"
LABEL org.opencontainers.image.source="https://github.com/stackabletech/postgresql-trino-gateway"
LABEL org.opencontainers.image.vendor="Stackable GmbH"
LABEL org.opencontainers.image.licenses="OSL-3.0"
LABEL org.opencontainers.image.documentation="https://docs.stackable.tech/"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.revision="${RELEASE}"
LABEL org.opencontainers.image.title="PostgreSQL-Trino Gateway"
LABEL org.opencontainers.image.description="A PostgreSQL wire-protocol gateway for Trino, enabling Power BI DirectQuery."

LABEL io.openshift.tags="ubi10,stackable,sdp,trino,postgresql,gateway"
LABEL io.k8s.description="A PostgreSQL wire-protocol gateway for Trino."
LABEL io.k8s.display-name="PostgreSQL-Trino Gateway"

COPY <<EOF /etc/dnf/dnf.conf
[main]
install_weak_deps=0
assumeyes=True
tsflags=nodocs
EOF

# hadolint ignore=DL3041
RUN <<EOF
microdnf update
microdnf install shadow-utils
groupadd --gid ${STACKABLE_USER_GID} --system ${STACKABLE_USER_NAME}
useradd \
    --no-log-init \
    --gid ${STACKABLE_USER_GID} \
    --uid ${STACKABLE_USER_UID} \
    --system \
    --create-home \
    --home-dir /stackable \
    ${STACKABLE_USER_NAME}
microdnf remove shadow-utils
microdnf clean all
rm -rf /var/cache/yum
chown -R ${STACKABLE_USER_UID}:0 /stackable
chmod -R g=u /stackable
EOF

COPY LICENSE /licenses/LICENSE
COPY --from=builder --chown=${STACKABLE_USER_UID}:0 /app/postgresql-trino-gateway /usr/local/bin/postgresql-trino-gateway

USER ${STACKABLE_USER_UID}
WORKDIR /stackable

EXPOSE 5432

ENTRYPOINT ["/usr/local/bin/postgresql-trino-gateway"]
