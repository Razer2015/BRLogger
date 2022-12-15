FROM curlimages/curl:latest as downloader

ARG REPO_URL
ARG TAG
ARG TARGETPLATFORM

WORKDIR /home/curl_user

RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then export ARCHITECTURE=x86_64-unknown-linux-gnu; elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then export ARCHITECTURE=aarch64-unknown-linux-gnu; else export ARCHITECTURE=aarch64-unknown-linux-gnu; fi \
    && curl -L -o bf4-brlogger.tar.gz ${REPO_URL}/releases/download/${TAG}/bf4-brlogger-${ARCHITECTURE}-${TAG}.tar.gz

RUN tar -xf bf4-brlogger.tar.gz

FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y ca-certificates

WORKDIR /app
COPY --from=downloader /home/curl_user/bf4-brlogger .
CMD ["./bf4-brlogger"]