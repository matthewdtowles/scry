# Multi-stage build for Rust
FROM rust:1.88-alpine AS base
WORKDIR /app
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

# Development stage
FROM base AS development
COPY Cargo.toml ./
COPY src ./src
RUN cargo build
CMD ["cargo", "run"]

# Build stage
# APP_VERSION is supplied by CI (computed from the PR title — see
# .github/scripts/next-version.sh); Cargo.toml itself stays at its
# 0.0.0-dev placeholder. The sed is scoped to [package] so dependency
# `version =` lines are untouched.
FROM base AS build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ARG APP_VERSION
RUN [ -z "$APP_VERSION" ] || sed -i "/^\[package\]/,/^\[/s/^version = .*/version = \"$APP_VERSION\"/" Cargo.toml
RUN cargo build --release

# Production stage
FROM alpine:latest AS production
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/target/release/scry ./scry
CMD ["./scry"]
