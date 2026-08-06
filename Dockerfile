# Build krply-server as a static, CGO-free binary and run it as the non-root
# distroless user (65532). The SQLite journal must be mounted writable at
# /data (see deploy/helm and deploy/compose).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/krply-server ./cmd/krply-server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/krply-server /usr/local/bin/krply-server
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/krply-server"]
