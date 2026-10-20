# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS builder

WORKDIR /src
RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/app ./cmd/${TARGET}

FROM alpine:3.21 AS runner

RUN apk add --no-cache ca-certificates tzdata curl

COPY --from=builder /bin/app /bin/app

USER 65534:65534
ENTRYPOINT ["/bin/app"]
