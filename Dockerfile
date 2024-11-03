FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o pg_flo ./cmd/pg_flo

FROM alpine:latest
COPY --from=builder /app/pg_flo /usr/local/bin/
ENTRYPOINT ["pg_flo"]
