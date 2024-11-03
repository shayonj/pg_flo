FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -v \
  -ldflags "-s -w \
  -X 'github.com/shayonj/pg_flo/cmd.version=${VERSION}' \
  -X 'github.com/shayonj/pg_flo/cmd.commit=${COMMIT}' \
  -X 'github.com/shayonj/pg_flo/cmd.date=${DATE}'" \
  -o pg_flo .

FROM alpine:latest
COPY --from=builder /app/pg_flo /usr/local/bin/
ENTRYPOINT ["pg_flo"]
