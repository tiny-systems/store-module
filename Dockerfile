# Build a Tiny Systems module image with no platform involvement.
# Mirrors the SDK's own build (module/tools/build): golang → distroless static.
# The module's cmd/main.go IS the controller-manager entrypoint.
# Build on the NATIVE runner arch (BUILDPLATFORM) and let Go cross-compile to
# the target arch — fast, no QEMU emulation. Only the tiny final stage is
# per-target.
FROM --platform=$BUILDPLATFORM golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
WORKDIR /manager
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -ldflags="-X github.com/tiny-systems/module/cli.versionID=${VERSION}" \
    -o /bin/manager ./cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /bin/manager /manager
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
USER 65532:65532
CMD ["/manager", "run"]
