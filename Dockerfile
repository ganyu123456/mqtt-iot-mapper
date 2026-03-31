# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates tzdata

# Copy all source and let go mod tidy generate go.sum if needed
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w" \
      -o /build/main ./cmd/main.go

# Runtime stage
FROM alpine:3.19

WORKDIR /kubeedge

RUN apk add --no-cache ca-certificates tzdata \
      mosquitto-clients \
      curl \
      iputils \
      busybox-extras

COPY --from=builder /build/main .

EXPOSE 7777

ENTRYPOINT ["/kubeedge/main"]
