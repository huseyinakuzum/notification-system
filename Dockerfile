ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cdc ./cmd/cdc \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/delivery ./cmd/delivery \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockprovider ./cmd/mockprovider

FROM alpine:3.20 AS api
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=builder /out/api /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]

FROM alpine:3.20 AS scheduler
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=builder /out/scheduler /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]

FROM alpine:3.20 AS cdc
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=builder /out/cdc /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]

FROM alpine:3.20 AS delivery
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=builder /out/delivery /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]

FROM alpine:3.20 AS mockprovider
RUN adduser -D -u 10001 app
USER app
COPY --from=builder /out/mockprovider /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
