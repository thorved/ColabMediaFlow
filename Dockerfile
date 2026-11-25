# syntax=docker/dockerfile:1
FROM golang:1.26-bookworm AS build

WORKDIR /src
ENV CGO_ENABLED=0

COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN mkdir -p /out/rootfs/data && \
    go build -trimpath -ldflags="-s -w" -o /out/colabmediaflow ./cmd/colabmediaflow

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/colabmediaflow /colabmediaflow
COPY --chown=65532:65532 --from=build /out/rootfs/ /

USER 65532:65532
WORKDIR /data
ENV LISTEN_ADDR=0.0.0.0:8080 \
    STATE_DB=/data/colabmediaflow.db
EXPOSE 8080
ENTRYPOINT ["/colabmediaflow"]
