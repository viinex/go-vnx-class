####################################################################3
## build vnx-class backend

FROM golang:1.25-alpine AS builder

WORKDIR /vnx-class

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /vnx-class.bin


###################################################################
## (optional) build viinex demo frontend

FROM node:20-alpine AS frontend-builder

RUN apk add --no-cache git make

WORKDIR /viinex-demo-ui

ARG CACHE_BUSTER

RUN git clone https://github.com/viinex/viinex-demo-ui .

RUN make env
RUN make build


#####################################################################
## build container image for vnx-class with binary and viinex demo ui

FROM alpine:latest
RUN apk --no-cache add ca-certificates

COPY --from=builder /vnx-class.bin /usr/sbin/vnx-class
COPY --from=frontend-builder /viinex-demo-ui/dist /usr/share/viinex/web

EXPOSE 8080

ENV STATIC=/usr/share/viinex/web/browser/en \
    ETCD_ENDPOINTS=etcd:2379 \
    # ETCD_USERNAME="vnxclass" \
    # ETCD_PASSWORD="vnxclass" \
    WAMP=0.0.0.0:8080 \
    PROMETHEUS_PUSH_URI=http://victoria-metrics:8428/api/v1/import/prometheus

USER 1001

CMD ["/usr/sbin/vnx-class"]
