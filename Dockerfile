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
FROM base AS build
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# Production stage
FROM alpine:latest AS production
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /app/target/release/scry ./scry
CMD ["./scry"]
